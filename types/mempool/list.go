package mempool

import (
	"github.com/cometbft/cometbft/libs/cmap"
	"github.com/cosmos/cosmos-sdk/types"
)

// sortedMap is a nonce->MempoolTx hash map with a heap based index to allow
// iterating over the contents in a nonce-incrementing way.
type sortedMap struct {
	items *cmap.ShardedMap
}

// newSortedMap creates a new nonce-sorted MempoolTx map.
func newSortedMap() *sortedMap {
	return &sortedMap{
		items: cmap.NewShardedMap(20000),
	}
}

// Get retrieves the current MempoolTxs associated with the given nonce.
func (m *sortedMap) Get(nonce uint64) *types.MempoolTx {
	tx, ok := m.items.Load(nonce)
	if ok {
		return tx.(*types.MempoolTx)
	}
	return nil
}

// Put inserts a new MempoolTx into the map, also updating the map's nonce
// index. If a MempoolTx already exists with the same nonce, it's overwritten.
func (m *sortedMap) Put(tx *types.MempoolTx) {
	nonce := tx.Nonce
	m.items.Store(nonce, tx, func() {

	})
}

// Remove deletes a MempoolTx from the maintained map, returning whether the
// MempoolTx was found.
func (m *sortedMap) Remove(nonce uint64) bool {
	return m.items.Delete(nonce, func(k any) {
	})
}

// list is a "list" of MempoolTxs belonging to an account, sorted by account
// nonce. The same type can be used both for storing contiguous MempoolTxs for
// the executable/pending queue; and for storing gapped MempoolTxs for the non-
// executable/future queue, with minor behavioral changes.
type list struct {
	txs *sortedMap // Heap indexed sorted hash map of the MempoolTxs
}

// newList creates a new MempoolTx list for maintaining nonce-indexable fast,
// gapped, sortable MempoolTx lists.
func newList() *list {
	return &list{
		txs: newSortedMap(),
	}
}

// Contains returns whether the  list contains a MempoolTx
// with the provided nonce.
func (l *list) Contains(nonce uint64) bool {
	return l.txs.Get(nonce) != nil
}

// Add tries to insert a new MempoolTx into the list, returning whether the
// MempoolTx was accepted, and if yes, any previous MempoolTx it replaced.
//
// If the new MempoolTx is accepted into the list, the lists' cost and gas
// thresholds are also potentially updated.
func (l *list) Add(tx *types.MempoolTx) (bool, *types.MempoolTx) {
	// If there's an older better MempoolTx, abort
	old := l.txs.Get(tx.Nonce)
	if old != nil {
		if old.GasFeeCapCmp(tx) >= 0 {
			return false, nil
		}
	}
	// Otherwise overwrite the old MempoolTx with the current one
	l.txs.Put(tx)
	return true, old
}
func (l *list) Put(tx *types.MempoolTx) {
	l.txs.Put(tx)
}

// Remove deletes a MempoolTx from the maintained list, returning whether the
// MempoolTx was found, and also returning any MempoolTx invalidated due to
// the deletion (strict mode only).
func (l *list) Remove(tx *types.MempoolTx) (bool, *types.MempoolTx) {
	// Remove the MempoolTx from the set
	nonce := tx.Nonce
	if removed := l.txs.Remove(nonce); !removed {
		return false, nil
	}
	return true, nil
}
