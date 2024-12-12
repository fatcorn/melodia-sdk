package mempool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	types2 "github.com/cosmos/cosmos-sdk/x/consensus/types"
	"sort"
	"strconv"
	"sync"
	"time"
)

var _ Mempool = (*NamespaceMempool)(nil)

// NamespaceMempool defines a namespace sort mempool.
type NamespaceMempool struct {
	mtx   sync.RWMutex
	pools map[uint64]Mempool
}

// NamespaceInterator  defines a namespace iterator.
type NamespaceInterator struct {
	NamespaceId uint64
	Iterator    Iterator
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		pools: make(map[uint64]Mempool),
	}
	return nsm
}

func (nsm *NamespaceMempool) SelectWithNsTable(ctx context.Context, nsByteTxs map[uint64][]*sdk.ByteTx) []NamespaceInterator {

	var (
		nsIters []NamespaceInterator
		wg      sync.WaitGroup
	)

	nsIters = make([]NamespaceInterator, 0)
	for namespaceId, byteTxs := range nsByteTxs {
		pool, ok := nsm.pools[namespaceId]
		if ok {
			wg.Add(1)
			go func(ctx context.Context, txs []*sdk.ByteTx, namespaceId uint64) {
				defer wg.Done()
				iter := pool.Select(ctx, nil)
				nsIters = append(nsIters, NamespaceInterator{
					NamespaceId: namespaceId,
					Iterator:    iter,
				})
			}(ctx, byteTxs, namespaceId)
		}
	}

	wg.Wait()

	sort.Slice(nsIters, func(i, j int) bool {
		iBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[i].NamespaceId)
		jBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[j].NamespaceId)
		return bytes.Compare(iBytes, jBytes) < 0
	})

	return nsIters
}

func (nsm *NamespaceMempool) SelectSortTxs(ctx context.Context, maxBytes, maxGas int64, nsByteTxs map[uint64][]*sdk.ByteTx, txEncodeBatch func(txs []sdk.Tx) ([]*sdk.ByteTx, error)) ([]*sdk.ByteTx, []*cmtproto.NamespaceTableEntries) {

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	t0 := time.Now()
	txsIterators := nsm.SelectWithNsTable(ctx, nsByteTxs)
	size := nsm.CountTx()
	sdkCtx.Logger().Info("3.1->sortTxs SelectWithNsTable time", "size", size, "use", time.Since(t0).String(), "height", sdkCtx.BlockHeight())
	byteTxs := make([]*sdk.ByteTx, 0, size)
	nsTable := make([]*cmtproto.NamespaceTableEntries, 0)
	offset := 0

	t1 := time.Now()
	for _, txIterator := range txsIterators {
		iterator := txIterator.Iterator
		namespaceId := txIterator.NamespaceId

		// If the iterator is nil use the NoOpMempool, get all namespace txs
		var nsTxs []*sdk.ByteTx
		if iterator == nil {
			nsTxs = nsByteTxs[namespaceId]
			byteTxs = append(byteTxs, nsTxs...)
		} else {
			txs := make([]sdk.Tx, 0)
			for iterator != nil {
				tx := iterator.Tx()
				txs = append(txs, tx)
				iterator = iterator.Next()
			}
			nsTxs, _ = txEncodeBatch(txs)
			byteTxs = append(byteTxs, nsTxs...)
		}

		offset += len(nsTxs)
		nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    int32(offset),
		})
	}

	sdkCtx.Logger().Info("3.2->sortTxs EncodeBatch time", "use", time.Since(t1).String(), "size", len(byteTxs), "nsTable", nsTable, "height", sdkCtx.BlockHeight())

	if maxBytes > 0 {
		t2 := time.Now()
		byteTxs, nsTable = sdk.ReapMaxBytesMaxGasNsTable(maxBytes, maxGas, byteTxs, nsTable)
		sdkCtx.Logger().Info("3.3->sortTxs ReapMaxBytesMaxGasNsTable time", "use", time.Since(t2).String(), "size", len(byteTxs), "reqSize", size, "nsTable", nsTable, "maxBytes", maxBytes, "maxGas", maxGas, "height", sdkCtx.BlockHeight())
	}

	return byteTxs, nsTable
}

func (nsm *NamespaceMempool) Init(ctx context.Context) Mempool {

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	namespaceId := sdkCtx.Namespace()
	pool, init := nsm.pools[namespaceId]
	if !init {
		nsm.mtx.Lock()
		pool, init = nsm.pools[namespaceId]
		if !init {
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
			nsm.pools[namespaceId] = pool
		}
		nsm.mtx.Unlock()
	}
	return pool
}

func (nsm *NamespaceMempool) GetPool(namespaceId uint64) Mempool {
	pool, init := nsm.pools[namespaceId]
	if !init {
		return nil
	}
	return pool
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.Tx) error {
	namespaceId := sdk.UnwrapSDKContext(ctx).Namespace()
	pool, init := nsm.pools[namespaceId]
	if !init {
		pool = nsm.Init(ctx)
	}
	return pool.Insert(ctx, tx)
}
func (nsm *NamespaceMempool) Select(context.Context, [][]byte) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	count := 0
	for _, mem := range nsm.pools {
		count += mem.CountTx()
	}
	return count
}
func (nsm *NamespaceMempool) CountTxByNamespaceId(namespaceId uint64) int {
	if pool, ok := nsm.pools[namespaceId]; ok {
		return pool.CountTx()
	}
	return 0
}
func (nsm *NamespaceMempool) RemoveWithCtx(ctx context.Context, tx sdk.Tx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	namespaceId := sdkCtx.Namespace()
	if pool, ok := nsm.pools[namespaceId]; ok {
		err := pool.Remove(tx)
		if err != nil {
			return nsm.Remove(tx)
		} else {
			return nil
		}
	}
	return nsm.Remove(tx)
}
func (nsm *NamespaceMempool) Remove(tx sdk.Tx) error {
	for _, mem := range nsm.pools {
		_ = mem.Remove(tx)
	}

	return nil
}
