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

// NamespaceSort defines a mempool that sorts transactions based on namespace.
type NamespaceSort struct {
	pools *sync.Map // A map to store pools indexed by namespace ID.
}

// NamespaceInterator defines an iterator for a specific namespace.
type NamespaceInterator struct {
	NamespaceId uint64   // Namespace ID
	Iterator    Iterator // Iterator for transactions in the namespace
}

// NewNamespaceSort creates a new NamespaceSort instance to manage transaction sorting by namespace.
func NewNamespaceSort() *NamespaceSort {
	nsm := &NamespaceSort{
		pools: &sync.Map{}, // Initializes the sync.Map to store pools.
	}
	return nsm
}

// GetPool retrieves or initializes a mempool for a specific namespace.
func (nsm *NamespaceSort) GetPool(ctx context.Context) Mempool {

	sdkCtx := sdk.UnwrapSDKContext(ctx) // Unwraps the SDK context to access relevant parameters.
	namespaceId := sdkCtx.Namespace()   // Fetches the namespace ID from the context.
	var pool Mempool
	if p, ok := nsm.pools.Load(namespaceId); ok {
		pool = p.(Mempool) // If the pool exists for this namespace, use it.
	} else {
		// If the namespace ID does not exist, initialize a new pool.
		sortMode := types2.SortModeGasPrice // Default sorting mode is based on gas price.
		if sdkCtx.ConsensusParams().Mempool != nil && len(sdkCtx.ConsensusParams().Mempool.Sort) > 0 {
			// If custom sorting rules are provided in the consensus params, use them.
			poolSort := make(map[string]string)
			_ = json.Unmarshal([]byte(sdkCtx.ConsensusParams().Mempool.Sort), &poolSort)
			sortType, _ := poolSort[strconv.FormatUint(namespaceId, 10)]
			sortMode = types2.SortMode(sortType) // Use the specific sorting mode for this namespace.
		}
		// Select the pool based on the sorting mode.
		switch sortMode {
		case types2.SortModeGasPrice:
			pool = DefaultPriceNonceMempool() // Sort by gas price and nonce.
		case types2.SortModeQueue:
			pool = DefaultSimpleSenderNonceMempool() // Sort based on the sender's nonce in a simple queue.
		default:
			pool = DefaultPriceNonceMempool() // Fallback to default gas price and nonce sorting.
		}
		nsm.pools.Store(namespaceId, pool) // Store the initialized pool in the map.
	}
	return pool
}

// SortTxs sorts transactions in the mempool based on their namespace and respective sorting criteria.
func (nsm *NamespaceSort) SortTxs(ctx context.Context, nsTxs map[uint64][]sdk.MempoolTx) []NamespaceInterator {

	var (
		nsIters []NamespaceInterator // List to hold namespace iterators.
		wg      sync.WaitGroup       // WaitGroup for concurrent goroutines.
	)

	sdkCtx := sdk.UnwrapSDKContext(ctx)     // Unwraps SDK context for access to namespace info.
	nsIters = make([]NamespaceInterator, 0) // Initializes the slice for iterators.

	// Iterate over all namespaces and transactions concurrently.
	for namespaceId, txs := range nsTxs {
		wg.Add(1)
		go func(ctx sdk.Context, txs []sdk.MempoolTx, namespaceId uint64) {
			defer wg.Done()                                       // Ensure the WaitGroup counter is decremented after the function finishes.
			pool := nsm.GetPool(sdkCtx.WithNamepace(namespaceId)) // Get the pool for this namespace.
			iter := pool.Select(ctx, txs)                         // Select transactions for this pool.
			nsIters = append(nsIters, NamespaceInterator{
				NamespaceId: namespaceId,
				Iterator:    iter, // Create a NamespaceInterator with the namespace and its iterator.
			})
		}(sdkCtx, txs, namespaceId)
	}

	wg.Wait() // Wait for all goroutines to finish.

	// Sort namespace iterators by namespace ID.
	if len(nsIters) > 0 {
		sort.Slice(nsIters, func(i, j int) bool {
			iBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[i].NamespaceId)
			jBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[j].NamespaceId)
			return bytes.Compare(iBytes, jBytes) < 0 // Compare namespaces in ascending order.
		})
	}
	return nsIters
}

// SelectTxs selects transactions based on the given parameters, applying sorters and selectors in order.
func (nsm *NamespaceSort) SelectTxs(ctx context.Context, maxBytes, maxGas int64, reqTxs []sdk.MempoolTx, reqNsTable []*cmtproto.NamespaceTableEntries, sortType types.SortType) sdk.NamespaceSelectedTxs {

	t0 := time.Now()                                        // Record start time for sorting transactions.
	sdkCtx := sdk.UnwrapSDKContext(ctx)                     // Unwrap SDK context to get namespace transactions.
	nsMemTxs := sdk.ListNamespaceMemTxs(reqTxs, reqNsTable) // List all transactions grouped by namespace.
	txsIterators := nsm.SortTxs(ctx, nsMemTxs)              // Sort transactions by namespace.
	sdkCtx.Logger().Info("2.1->sortTxs sort time", "use", time.Since(t0).String(), "reqSize", len(reqTxs), "reqNsTable", reqNsTable, "height", sdkCtx.BlockHeight())

	t1 := time.Now()                                              // Record start time for selecting transactions.
	selectedTxs := make([]sdk.MempoolTx, 0)                       // Slice to hold selected transactions.
	selectedNsTable := make([]*cmtproto.NamespaceTableEntries, 0) // Slice to hold namespace table entries.
	offset := 0

	// Process each iterator for every namespace.
	for _, txIterator := range txsIterators {
		iterator := txIterator.Iterator
		namespaceId := txIterator.NamespaceId

		// If the iterator is nil, use all transactions in the namespace.
		txsLen := 0
		if iterator == nil {
			txs := nsMemTxs[namespaceId]
			selectedTxs = append(selectedTxs, txs...)
			txsLen = len(txs)
		} else {
			// If the iterator exists, iterate over transactions and select them.
			for iterator != nil {
				tx := iterator.Tx()
				selectedTxs = append(selectedTxs, tx)
				txsLen++
				iterator = iterator.Next()
			}
		}
		offset += txsLen
		// Add namespace table entry for the selected transactions.
		selectedNsTable = append(selectedNsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    int32(offset),
		})
	}

	sdkCtx.Logger().Info("2.2->sortTxs txsIterators time", "use", time.Since(t1).String(), "size", len(selectedTxs), "reqSize", len(reqTxs), "height", sdkCtx.BlockHeight())

	t2 := time.Now() // Record start time for applying selectors.

	nsSelectedTxs := sdk.NamespaceSelectedTxs{
		Txs:             selectedTxs,
		NsTable:         selectedNsTable,
		ReplacedTxs:     make([]sdk.MempoolTx, 0),
		ReplacedNsTable: make([]*cmtproto.NamespaceTableEntries, 0),
	}

	// If the sorting type requires additional selection logic, apply selectors.
	if sortType == types.SortAndSelect {
		selectors := []func(sdk.Context, sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs{
			selector.BaseFeeSelector,
			selector.InterChainSelector,
			selector.NonceSelector,
			func(ctx sdk.Context, nsSelectedTxs sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs {
				return selector.LimitSelector(ctx, maxBytes, maxGas, nsSelectedTxs)
			},
		}
		// Apply selectors sequentially to refine the selected transactions.
		for _, selectFn := range selectors {
			nsSelectedTxs = selectFn(sdkCtx, nsSelectedTxs)
		}
	}

	sdkCtx.Logger().Info("2.3->sortTxs selectors time", "use", time.Since(t2).String(), "size", len(nsSelectedTxs.Txs), "reqSize", len(reqTxs), "height", sdkCtx.BlockHeight())

	return nsSelectedTxs
}

// Select is the main function that selects transactions for the mempool, along with namespace table entries and transaction orders.
func (nsm *NamespaceSort) Select(ctx context.Context, maxBytes, maxGas int64, reqTxs []sdk.MempoolTx, reqNsTable []*cmtproto.NamespaceTableEntries, sortType types.SortType) ([][]byte, []*cmtproto.NamespaceTableEntries, *cmtproto.Orders, [][]byte, []*cmtproto.NamespaceTableEntries) {

	sdkCtx := sdk.UnwrapSDKContext(ctx)

	// Select transactions based on the defined parameters.
	nsSelectedTxs := nsm.SelectTxs(ctx, maxBytes, maxGas, reqTxs, reqNsTable, sortType)

	selectedTxs := nsSelectedTxs.Txs
	selectedNsTable := nsSelectedTxs.NsTable

	t0 := time.Now() // Record start time for preparing transaction orders.
	var (
		txs     = make([][]byte, 0)                          // Slice to hold final selected transactions in byte form.
		nsTable = make([]*cmtproto.NamespaceTableEntries, 0) // Namespace table entries for selected transactions.
		orders  = &cmtproto.Orders{
			NsTransactionOrders: &cmtproto.NsTransactionOrders{
				Entries: make([]*cmtproto.NsTransactionOrderEntry, 0)}, // Initialize orders struct.
			NsInterChainTransactionOrders: &cmtproto.NsInterChainTransactionOrders{
				Entries: make([]*cmtproto.NsInterChainTransactionOrderEntry, 0),
			},
		}
		offset                    = 0
		txIndex                   = 0
		iterOffset                = int32(0)
		selectedInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
	)

	// Function to handle inter-chain transaction order entries.
	handleNsInterChainTransactionOrders := func(memTx sdk.MempoolTx, txIdx int, namespaceId uint64) {
		tx := memTx.Tx
		txBz := memTx.TxBytes
		txIdBz, isCross, isFirst := memTx.CrossNamespaceTransactionIdentifierHandler(sdkCtx.WithTxBytes(txBz), tx)

		if isCross {
			txId := tmbytes.HexBytes(txIdBz).String()
			if interTxs, ok := selectedInterChainPairTxs[txId]; ok {
				// If the inter-chain transaction pair is already recorded, add the new transaction.
				interTxs = append(interTxs, &cmtproto.NsInterChainTransactionIndex{
					Namespace: namespaceId,
					Index:     int32(txIdx),
				})

				// Reorder the transactions depending on their position.
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
				// Record the new inter-chain transaction pair.
				selectedInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
					{
						Namespace: namespaceId,
						Index:     int32(txIdx),
					},
				}
			}
		}
	}

	// Process selected transactions and assign them to orders.
	for _, nsEntry := range selectedNsTable {

		memTxs := selectedTxs[iterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace

		// Static analysis and dependencies of transactions.
		var (
			addressIndexMap      = make(map[string]int) // Map to store address indices.
			lastTxIndexMap       = make(map[string]int) // Map to track last seen transaction indices.
			nsTransactionIndexes = make([]*cmtproto.NsTransactionIndexes, 0, len(memTxs))
			contractIsSerialMap  = make(map[string]bool)
		)

		// Handle transaction dependencies and ordering.
		handleNsTransactionDependOrders := func(txIdx int, namespaceId uint64, from string, to string) {
			if lastToIdx, toExists := lastTxIndexMap[to]; toExists {
				if lastFromIdx, fromExists := lastTxIndexMap[from]; !fromExists || lastFromIdx != lastToIdx {
					// Record dependency order for transactions.
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

		// Function to handle transaction order entries.
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
					if isContractSerial, ok := contractIsSerialMap[to]; ok {
						return isContractSerial
					}
					isContract, isContractSerial := tx.ContractStaticAnalysisResultHandler(sdkCtx.WithNamepace(namespaceId), to)
					if isContract {
						contractIsSerialMap[to] = isContractSerial
						return isContractSerial
					}
					contractIsSerialMap[to] = true
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
				// Both from and to addresses exist in the index, so add to current index.
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))

				// Check if the transaction should be serial dependent.
				if toIsSerial(tx) {
					isToSerial = true
				}
			} else if fromExists {
				// Only the from address exists, add to the current index.
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
				addressIndexMap[to] = curIdx
			} else if toExists {
				// Only the to address exists, add from index.
				if toIsSerial(tx) {
					isToSerial = true
					curIdx = toIdx
					nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
					addressIndexMap[from] = curIdx
				} else {
					// Create a new from index.
					nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
					curIdx = len(nsTransactionIndexes) - 1
					addressIndexMap[from] = curIdx
				}
			} else {
				// Neither address exists, create new indices for both.
				nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
				curIdx = len(nsTransactionIndexes) - 1
				addressIndexMap[from] = curIdx
				addressIndexMap[to] = curIdx
			}
		}

		// Process each transaction and handle its dependencies.
		for _, memTx := range memTxs {
			txs = append(txs, memTx.TxBytes) // Add the transaction to the result.
			offset++
			handleNsTransactionOrders(memTx, txIndex, namespaceId) // Handle dependencies and orders.
			txIndex++
		}

		// If there are transaction indexes, append them to the namespace table and orders.
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

		// Update the iterator offset for the next namespace.
		iterOffset = nsEntry.Offset
	}

	sdkCtx.Logger().Info("2.4->sortTxs orders time", "use", time.Since(t0).String(), "size", len(selectedTxs), "reqSize", len(reqTxs), "height", sdkCtx.BlockHeight())

	t1 := time.Now()

	var (
		replacedTxs        = make([][]byte, 0)
		replacedNsTable    = make([]*cmtproto.NamespaceTableEntries, 0)
		replacedOffset     = 0
		replacedIterOffset = int32(0)
	)
	for _, nsEntry := range nsSelectedTxs.ReplacedNsTable {
		memTxs := nsSelectedTxs.ReplacedTxs[replacedIterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace
		for _, memTx := range memTxs {
			replacedTxs = append(replacedTxs, memTx.TxBytes)
			replacedOffset++
		}
		replacedNsTable = append(replacedNsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    int32(replacedOffset),
		})
		replacedIterOffset = nsEntry.Offset
	}
	if len(replacedTxs) > 0 {
		sdkCtx.Logger().Info("2.5->sortTxs replaced time", "use", time.Since(t1).String(), "size", len(nsSelectedTxs.Txs), "reqSize", len(reqTxs), "replacedSize", len(replacedTxs), "height", sdkCtx.BlockHeight())
	}
	return txs, nsTable, orders, replacedTxs, replacedNsTable // Return the sorted transactions, namespace table, and order details.
}
