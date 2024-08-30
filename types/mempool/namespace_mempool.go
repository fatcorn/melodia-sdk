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
	pools sync.Map
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
		pools: sync.Map{},
	}
	return nsm
}

func (nsm *NamespaceMempool) SelectWithNsTable(ctx context.Context, txs [][]byte, nsTable []*cmtproto.NamespaceTableEntries) ([]NamespaceInterator, map[uint64][][]byte) {

	var (
		nsIters []NamespaceInterator
		nsTxs   map[uint64][][]byte
		wg      sync.WaitGroup
	)

	nsIters = make([]NamespaceInterator, 0)
	nsTxs = make(map[uint64][][]byte)
	lastOffset := int32(0)
	for _, entry := range nsTable {
		namespaceTxs := txs[lastOffset:entry.Offset]
		namespaceId := entry.Namespace
		pool, ok := nsm.pools.Load(namespaceId)
		if ok {
			wg.Add(1)
			go func(ctx context.Context, txs [][]byte, namespaceId uint64) {
				defer wg.Done()
				iter := pool.(Mempool).Select(ctx, txs)
				nsIters = append(nsIters, NamespaceInterator{
					NamespaceId: namespaceId,
					Iterator:    iter,
				})
			}(ctx, namespaceTxs, namespaceId)
		}
		nsTxs[namespaceId] = namespaceTxs
		lastOffset = entry.Offset
	}

	wg.Wait()

	sort.Slice(nsIters, func(i, j int) bool {
		iBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[i].NamespaceId)
		jBytes := binary.LittleEndian.AppendUint64([]byte{}, nsIters[j].NamespaceId)
		return bytes.Compare(iBytes, jBytes) < 0
	})

	return nsIters, nsTxs
}

func (nsm *NamespaceMempool) Init(ctx context.Context) Mempool {

	nsm.mtx.Lock()
	defer nsm.mtx.Unlock()

	sdkCtx := sdk.UnwrapSDKContext(ctx)
	namespaceId := sdkCtx.Namespace()
	pool, init := nsm.pools.Load(namespaceId)
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
		nsm.pools.Store(namespaceId, pool)
	}
	return pool.(Mempool)
}

func (nsm *NamespaceMempool) GetPool(namespaceId uint64) Mempool {
	pool, init := nsm.pools.Load(namespaceId)
	if !init {
		return nil
	}
	return pool.(Mempool)
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.Tx) error {
	namespaceId := sdk.UnwrapSDKContext(ctx).Namespace()
	pool, init := nsm.pools.Load(namespaceId)
	if !init {
		pool = nsm.Init(ctx)
	}
	return pool.(Mempool).Insert(ctx, tx)
}
func (nsm *NamespaceMempool) Select(context.Context, [][]byte) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
	count := 0
	nsm.pools.Range(func(key, pool interface{}) bool {
		count += pool.(Mempool).CountTx()
		return true
	})
	return count
}
func (nsm *NamespaceMempool) RemoveByNamespaceId(namespaceId uint64, tx sdk.Tx) error {
	nsm.mtx.RLock()
	defer nsm.mtx.RUnlock()
	if pool, ok := nsm.pools.Load(namespaceId); ok {
		err := pool.(Mempool).Remove(tx)
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
	nsm.pools.Range(func(key, pool interface{}) bool {
		pool.(Mempool).Remove(tx)
		return true
	})
	return nil
}
