package mempool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	tmbytes "github.com/cometbft/cometbft/libs/bytes"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/mempool/selector"
	types2 "github.com/cosmos/cosmos-sdk/x/consensus/types"
	"sort"
	"strconv"
	"sync"
	"time"
)

// NamespaceSort defines a namespace sort mempool.
type NamespaceSort struct {
	pools *sync.Map
}

// NamespaceInterator  defines a namespace iterator.
type NamespaceInterator struct {
	NamespaceId uint64
	Iterator    Iterator
}

// NewNamespaceSort creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceSort() *NamespaceSort {
	nsm := &NamespaceSort{
		pools: &sync.Map{},
	}
	return nsm
}

func (nsm *NamespaceSort) GetPool(ctx context.Context) Mempool {

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	namespaceId := sdkCtx.Namespace()
	var pool Mempool
	if p, ok := nsm.pools.Load(namespaceId); ok {
		pool = p.(Mempool)
	} else {
		// If the namespaceId does not exist, initialize it
		sortMode := types2.SortModeGasPrice
		if sdkCtx.ConsensusParams().Mempool != nil && len(sdkCtx.ConsensusParams().Mempool.Sort) > 0 {
			poolSort := make(map[string]string)
			_ = json.Unmarshal([]byte(sdkCtx.ConsensusParams().Mempool.Sort), &poolSort)
			sortType, _ := poolSort[strconv.FormatUint(namespaceId, 10)]
			sortMode = types2.SortMode(sortType)
		}
		switch sortMode {
		case types2.SortModeGasPrice:
			pool = DefaultPriceNonceMempool()
		case types2.SortModeQueue:
			pool = NewSenderNonceMempool()
		default:
			pool = DefaultPriceNonceMempool()
		}
		nsm.pools.Store(namespaceId, pool)
	}
	return pool
}
func (nsm *NamespaceSort) SortTxs(ctx context.Context, nsTxs map[uint64][]sdk.MempoolTx) []NamespaceInterator {

	var (
		nsIters []NamespaceInterator
		wg      sync.WaitGroup
	)

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	nsIters = make([]NamespaceInterator, 0)
	for namespaceId, txs := range nsTxs {
		wg.Add(1)
		go func(ctx sdk.Context, txs []sdk.MempoolTx, namespaceId uint64) {
			defer wg.Done()
			pool := nsm.GetPool(sdkCtx.WithNamepace(namespaceId))
			iter := pool.Select(ctx, txs)
			nsIters = append(nsIters, NamespaceInterator{
				NamespaceId: namespaceId,
				Iterator:    iter,
			})
		}(sdkCtx, txs, namespaceId)
	}

	wg.Wait()

	sort.Slice(nsIters, func(i, j int) bool {
		iBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[i].NamespaceId)
		jBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[j].NamespaceId)
		return bytes.Compare(iBytes, jBytes) < 0
	})

	return nsIters
}

func (nsm *NamespaceSort) SelectTxs(ctx context.Context, maxBytes, maxGas int64, reqTxs []sdk.MempoolTx, reqNsTable []*cmtproto.NamespaceTableEntries, sortType types.SortType) sdk.NamespaceSelectedTxs {

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	t0 := time.Now()
	nsMemTxs := sdk.ListNamespaceMemTxs(reqTxs, reqNsTable)

	txsIterators := nsm.SortTxs(ctx, nsMemTxs)
	sdkCtx.Logger().Info("2.1->sortTxs sort time", "use", time.Since(t0).String(), "reqSize", len(reqTxs), "reqNsTable", reqNsTable, "height", sdkCtx.BlockHeight())

	selectedTxs := make([]sdk.MempoolTx, 0)
	selectedNsTable := make([]*cmtproto.NamespaceTableEntries, 0)
	offset := 0

	t1 := time.Now()
	for _, txIterator := range txsIterators {
		iterator := txIterator.Iterator
		namespaceId := txIterator.NamespaceId

		// If the iterator is nil use the NoOpMempool, get all namespace txs
		txsLen := 0
		if iterator == nil {
			txs := nsMemTxs[namespaceId]
			selectedTxs = append(selectedTxs, txs...)
			txsLen = len(txs)
		} else {
			for iterator != nil {
				tx := iterator.Tx()
				selectedTxs = append(selectedTxs, tx)
				txsLen++
				iterator = iterator.Next()
			}
		}
		offset += txsLen
		selectedNsTable = append(selectedNsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    int32(offset),
		})
	}

	nsSelectedTxs := sdk.NamespaceSelectedTxs{
		Txs:     selectedTxs,
		NsTable: selectedNsTable,
	}

	sdkCtx.Logger().Info("2.2->sortTxs txsIterators time", "use", time.Since(t1).String(), "size", len(nsSelectedTxs.Txs), "reqSize", len(reqTxs), "height", sdkCtx.BlockHeight())

	t2 := time.Now()

	if sortType == types.SortAndSelect {
		// Define selectors in an array to apply them in sequence
		selectors := []func(sdk.Context, sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs{
			func(ctx sdk.Context, nsSelectedTxs sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs {
				return selector.LimitSelector(ctx, nsSelectedTxs, maxBytes, maxGas)
			},
			selector.BaseFeeSelector,
			selector.InterChainSelector,
			selector.NonceSelector,
		}
		// Apply all selectors in sequence
		for _, selectFn := range selectors {
			nsSelectedTxs = selectFn(sdkCtx, nsSelectedTxs)
		}
	}

	sdkCtx.Logger().Info("2.3->sortTxs selectors time", "use", time.Since(t2).String(), "size", len(nsSelectedTxs.Txs), "reqSize", len(reqTxs), "height", sdkCtx.BlockHeight())

	return nsSelectedTxs
}

func (nsm *NamespaceSort) Select(ctx context.Context, maxBytes, maxGas int64, reqTxs []sdk.MempoolTx, reqNsTable []*cmtproto.NamespaceTableEntries, sortType types.SortType) ([][]byte, []*cmtproto.NamespaceTableEntries, *cmtproto.Orders, [][]byte, []*cmtproto.NamespaceTableEntries) {

	sdkCtx := sdk.UnwrapSDKContext(ctx)

	nsSelectedTxs := nsm.SelectTxs(ctx, maxBytes, maxGas, reqTxs, reqNsTable, sortType)

	var (
		//sort
		txs     = make([][]byte, 0)
		nsTable = make([]*cmtproto.NamespaceTableEntries, 0)
		orders  = &cmtproto.Orders{
			NsTransactionOrders: &cmtproto.NsTransactionOrders{
				Entries: make([]*cmtproto.NsTransactionOrderEntry, 0)},
			NsInterChainTransactionOrders: &cmtproto.NsInterChainTransactionOrders{
				Entries: make([]*cmtproto.NsInterChainTransactionOrderEntry, 0),
			},
		}
		offset = 0
		//global
		txIndex                   = 0
		iterOffset                = int32(0)
		selectedInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
	)
	handleNsInterChainTransactionOrders := func(memTx sdk.MempoolTx, txIdx int, namespaceId uint64) {
		tx := memTx.Tx
		txBz := memTx.TxBytes
		txIdBz, isCross, isFirst := memTx.CrossNamespaceTransactionIdentifierHandler(sdkCtx.WithTxBytes(txBz), tx)

		if isCross {
			txId := tmbytes.HexBytes(txIdBz).String()
			if interTxs, ok := selectedInterChainPairTxs[txId]; ok {
				interTxs = append(interTxs, &cmtproto.NsInterChainTransactionIndex{
					Namespace: namespaceId,
					Index:     int32(txIdx),
				})

				idxes := make([]*cmtproto.NsInterChainTransactionIndex, 2)

				if isFirst {
					idxes[0] = interTxs[1]
					idxes[1] = interTxs[0]
				} else {
					idxes[0] = interTxs[0]
					idxes[1] = interTxs[1]
				}

				entry := &cmtproto.NsInterChainTransactionOrderEntry{
					Indexes: idxes,
				}
				orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
			} else {
				selectedInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
					{
						Namespace: namespaceId,
						Index:     int32(txIdx),
					},
				}
			}
		}
	}

	t0 := time.Now()

	for _, nsEntry := range nsSelectedTxs.NsTable {

		memTxs := nsSelectedTxs.Txs[iterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace

		// Static analysis
		var (
			addressIndexMap      = make(map[string]int)
			lastTxIndexMap       = make(map[string]int)
			nsTransactionIndexes = make([]*cmtproto.NsTransactionIndexes, 0, len(memTxs))
		)
		handleNsTransactionDependOrders := func(txIdx int, namespaceId uint64, from string, to string) {
			if lastToIdx, toExists := lastTxIndexMap[to]; toExists {
				if lastFromIdx, fromExists := lastTxIndexMap[from]; !fromExists || lastFromIdx != lastToIdx {
					entry := &cmtproto.NsInterChainTransactionOrderEntry{
						Indexes: []*cmtproto.NsInterChainTransactionIndex{
							{
								Namespace: namespaceId,
								Index:     int32(lastToIdx),
							},
							{
								Namespace: namespaceId,
								Index:     int32(txIdx),
							},
						},
					}
					orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
				}
			}
			lastTxIndexMap[from] = txIdx
			lastTxIndexMap[to] = txIdx
		}

		handleNsTransactionOrders := func(tx sdk.MempoolTx, txIdx int, namespaceId uint64) {
			var (
				from       = tx.From
				to         = tx.To
				fromIdx    int
				toIdx      int
				fromExists bool
				toExists   bool
				curIdx     int
				isToSerial bool
				toIsSerial = func(tx sdk.MempoolTx) bool {
					isContract, isContractSerial := tx.ContractStaticAnalysisResultHandler(sdkCtx.WithNamepace(namespaceId), to)
					if isContract {
						return isContractSerial
					}
					return true
				}
			)

			defer func() {
				if isToSerial {
					handleNsTransactionDependOrders(txIdx, namespaceId, from, to)
				}
				handleNsInterChainTransactionOrders(tx, txIdx, namespaceId)
			}()

			fromIdx, fromExists = addressIndexMap[from]
			toIdx, toExists = addressIndexMap[to]

			if fromExists && toExists {
				//Both exist,add from index
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))

				//Check if they should be depends
				if toIsSerial(tx) {
					isToSerial = true
				}
			} else if fromExists {
				// Only from exists, add to index
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
				addressIndexMap[to] = curIdx
			} else if toExists {
				// Only to exists, add from index
				if toIsSerial(tx) {
					isToSerial = true
					//append to index
					curIdx = toIdx
					nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
					addressIndexMap[from] = curIdx
				} else {
					//create new from index
					nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
					curIdx = len(nsTransactionIndexes) - 1
					addressIndexMap[from] = curIdx
				}
			} else {
				// Neither exists, create both indices
				//create new from & to index
				nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
				curIdx = len(nsTransactionIndexes) - 1
				addressIndexMap[from] = curIdx
				addressIndexMap[to] = curIdx
			}
		}

		for _, memTx := range memTxs {
			txs = append(txs, memTx.TxBytes)
			offset++
			handleNsTransactionOrders(memTx, txIndex, namespaceId)
			txIndex++
		}
		if len(nsTransactionIndexes) > 0 {
			nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
				Namespace: namespaceId,
				Offset:    int32(offset),
			})
			orders.NsTransactionOrders.Entries = append(orders.NsTransactionOrders.Entries, &cmtproto.NsTransactionOrderEntry{
				Namespace: namespaceId,
				Indexes:   nsTransactionIndexes,
			})
		}
		iterOffset = nsEntry.Offset
	}
	sdkCtx.Logger().Info("2.4->sortTxs orders time", "use", time.Since(t0).String(), "size", len(nsSelectedTxs.Txs), "reqSize", len(reqTxs), "height", sdkCtx.BlockHeight())

	t1 := time.Now()

	var (
		replaceTxs        = make([][]byte, 0)
		replaceNsTable    = make([]*cmtproto.NamespaceTableEntries, 0)
		replaceOffset     = 0
		replaceIterOffset = int32(0)
	)
	for _, nsEntry := range nsSelectedTxs.ReplaceNsTable {
		memTxs := nsSelectedTxs.ReplaceTxs[replaceIterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace
		for _, memTx := range memTxs {
			replaceTxs = append(replaceTxs, memTx.TxBytes)
			replaceOffset++
		}
		replaceNsTable = append(replaceNsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    int32(replaceOffset),
		})
		replaceIterOffset = nsEntry.Offset
	}
	if len(replaceTxs) > 0 {
		sdkCtx.Logger().Info("2.5->sortTxs replace time", "use", time.Since(t1).String(), "size", len(nsSelectedTxs.Txs), "reqSize", len(reqTxs), "replaceSize", len(replaceTxs), "height", sdkCtx.BlockHeight())
	}

	return txs, nsTable, orders, replaceTxs, replaceNsTable
}
