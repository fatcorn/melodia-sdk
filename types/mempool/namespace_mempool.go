package mempool

import (
	"context"
	"encoding/json"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	types2 "github.com/cosmos/cosmos-sdk/x/consensus/types"
	"strconv"
	"sync"
)

var _ Mempool = (*NamespaceMempool)(nil)

// NamespaceMempool defines a namespace sort mempool.
type NamespaceMempool struct {
	mtx   sync.Mutex
	pools map[uint64]Mempool
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		pools: make(map[uint64]Mempool),
	}
	return nsm
}

func (nsm *NamespaceMempool) SelectWithNsTable(ctx context.Context, txs [][]byte, nsTable []*cmtproto.NamespaceTableEntries) (map[uint64]Iterator, map[uint64][][]byte) {

	var (
		nsIter map[uint64]Iterator
		wg     sync.WaitGroup
	)

	nsIter = make(map[uint64]Iterator)
	nsTxs := types.ListNamespaceTxsWithNSTable(txs, nsTable)
	for namespaceId, namespaceTxs := range nsTxs {
		pool, ok := nsm.pools[namespaceId]
		if ok {
			wg.Add(1)
			go func(ctx context.Context, txs [][]byte, namespaceId uint64) {
				defer wg.Done()
				iter := pool.Select(ctx, txs)
				nsm.mtx.Lock()
				defer nsm.mtx.Unlock()
				nsIter[namespaceId] = iter
			}(ctx, namespaceTxs, namespaceId)
		}
	}
	wg.Wait()
	return nsIter, nsTxs
}

func (nsm *NamespaceMempool) Init(ctx context.Context) Mempool {

	nsm.mtx.Lock()
	defer nsm.mtx.Unlock()

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	namespaceId := sdkCtx.Namespace()
	pool, init := nsm.pools[namespaceId]
	if !init {
		// If the namespaceId does not exist, initialize it
		sortMode := types2.SortModeNonce
		if len(sdkCtx.ConsensusParams().Mempool.Sort) > 0 {
			poolSort := make(map[string]string)
			_ = json.Unmarshal([]byte(sdkCtx.ConsensusParams().Mempool.Sort), &poolSort)
			sortType, _ := poolSort[strconv.FormatUint(namespaceId, 10)]
			sortMode = types2.SortMode(sortType)
		}
		switch sortMode {
		case types2.SortModeGasPrice:
			pool = DefaultPriorityMempool()
		case types2.SortModeQueue:
			pool = NoOpMempool{}
		case types2.SortModeNonce:
			pool = NewSenderNonceMempool()
		default:
			pool = NewSenderNonceMempool()
		}
		nsm.pools[namespaceId] = pool
	}
	return pool
}

func (nsm *NamespaceMempool) GetPool(namespaceId uint64) Mempool {
	nsm.mtx.Lock()
	defer nsm.mtx.Unlock()
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
	nsm.mtx.Lock()
	defer nsm.mtx.Unlock()
	count := 0
	for _, mem := range nsm.pools {
		count += mem.CountTx()
	}
	return count
}
func (nsm *NamespaceMempool) RemoveByNamespaceId(namespaceId uint64, tx sdk.Tx) error {
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
