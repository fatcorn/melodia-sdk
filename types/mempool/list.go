package mempool

import (
	"github.com/cometbft/cometbft/libs/cmap"
	"github.com/cosmos/cosmos-sdk/types"
)

// list is a "list" of MempoolTxs belonging to an account, sorted by account
// nonce. The same type can be used both for storing contiguous MempoolTxs for
// the executable/pending queue; and for storing gapped MempoolTxs for the non-
// executable/future queue, with minor behavioral changes.
type list struct {
	txs *cmap.ShardedMap // Heap indexed sorted hash map of the MempoolTxs
}

// newList creates a new MempoolTx list for maintaining nonce-indexable fast,
// gapped, sortable MempoolTx lists.
func newList() *list {
	return &list{
		txs: cmap.NewShardedMap(20000),
	}
}

// Contains returns whether the  list contains a MempoolTx
// with the provided nonce.
func (l *list) Contains(nonce uint64) bool {
	_, ok := l.txs.Load(nonce)
	return ok
}

// Add tries to insert a new MempoolTx into the list, returning whether the
// MempoolTx was accepted, and if yes, any previous MempoolTx it replaced.
//
// If the new MempoolTx is accepted into the list, the lists' cost and gas
// thresholds are also potentially updated.
func (l *list) Add(tx *types.MempoolTx) (bool, *types.MempoolTx) {
	// If there's an older better MempoolTx, abort
	old, ok := l.txs.Load(tx.Nonce)
	if ok {
		if old.(*types.MempoolTx).GasFeeCapCmp(tx) >= 0 {
			return false, nil
		}
	}
	// Otherwise overwrite the old MempoolTx with the current one
	l.txs.Store(tx.Nonce, tx, func() {})
	return true, old.(*types.MempoolTx)
}
func (l *list) Put(tx *types.MempoolTx) {
	l.txs.Store(tx.Nonce, tx, func() {})
}

// Remove deletes a MempoolTx from the maintained list, returning whether the
// MempoolTx was found, and also returning any MempoolTx invalidated due to
// the deletion (strict mode only).
func (l *list) Remove(tx *types.MempoolTx) (bool, *types.MempoolTx) {
	// Remove the MempoolTx from the set
	nonce := tx.Nonce
	if removed := l.txs.Delete(nonce, func(any any) {}); !removed {
		return false, nil
	}
	return true, nil
}
