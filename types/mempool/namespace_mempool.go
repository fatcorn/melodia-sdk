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

func (nsm *NamespaceMempool) SelectWithNsTable(ctx context.Context, txs [][]byte, nsTable []*cmtproto.NamespaceTableEntries) []NamespaceInterator {

	var (
		nsIters []NamespaceInterator
		wg      sync.WaitGroup
	)

	nsIters = make([]NamespaceInterator, 0)
	lastOffset := int32(0)
	for _, entry := range nsTable {
		namespaceTxs := txs[lastOffset:entry.Offset]
		namespaceId := entry.Namespace
		pool, ok := nsm.pools[namespaceId]
		if ok {
			wg.Add(1)
			go func(ctx context.Context, txs [][]byte, namespaceId uint64) {
				defer wg.Done()
				iter := pool.Select(ctx, txs)
				nsm.mtx.Lock()
				defer nsm.mtx.Unlock()
				nsIters = append(nsIters, NamespaceInterator{
					NamespaceId: namespaceId,
					Iterator:    iter,
				})
			}(ctx, namespaceTxs, namespaceId)
		}
		lastOffset = entry.Offset
	}

	wg.Wait()

	sort.Slice(nsIters, func(i, j int) bool {
		iBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[i].NamespaceId)
		jBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[j].NamespaceId)
		return bytes.Compare(iBytes, jBytes) < 0
	})

	return nsIters
}

func (nsm *NamespaceMempool) Init(ctx context.Context) Mempool {

	nsm.mtx.Lock()
	defer nsm.mtx.Unlock()

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	namespaceId := sdkCtx.Namespace()
	pool, init := nsm.pools[namespaceId]
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
			pool = NoOpMempool{}
		default:
			pool = DefaultPriceNonceMempool()
		}
		nsm.pools[namespaceId] = pool
	}
	return pool
}

func (nsm *NamespaceMempool) GetPool(namespaceId uint64) Mempool {
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
	pool, init := nsm.pools[namespaceId]
	if !init {
		return nil
	}
	return pool
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.Tx) error {
	namespaceId := sdk.UnwrapSDKContext(ctx).Namespace()
	nsm.mtx.RLock()
	pool, init := nsm.pools[namespaceId]
	nsm.mtx.RUnlock()
	if !init {
		pool = nsm.Init(ctx)
	}
	return pool.Insert(ctx, tx)
}
func (nsm *NamespaceMempool) Select(context.Context, [][]byte) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
	count := 0
	for _, mem := range nsm.pools {
		count += mem.CountTx()
	}
	return count
}
func (nsm *NamespaceMempool) CountTxByNamespaceId(namespaceId uint64) int {
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
	if pool, ok := nsm.pools[namespaceId]; ok {
		return pool.CountTx()
	}
	return 0
}
func (nsm *NamespaceMempool) RemoveWithCtx(ctx context.Context, tx sdk.Tx) error {
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
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
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
	for _, mem := range nsm.pools {
		_ = mem.Remove(tx)
	}

	return nil
}
