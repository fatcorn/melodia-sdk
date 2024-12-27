package mempool

import (
	"context"
	"errors"
	"github.com/cometbft/cometbft/libs/cmap"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"strconv"
	"sync"
)

var _ Mempool = (*NamespaceMempool)(nil)

// NamespaceMempool defines a namespace sort mempool.
type NamespaceMempool struct {
	signersCheckNonce map[uint64]*noncer
	signersFinalNonce map[uint64]*noncer
	pendingTxs        map[string]*list
	mux               sync.RWMutex
	txsSigner         *cmap.ShardedMap
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		signersCheckNonce: make(map[uint64]*noncer),
		signersFinalNonce: make(map[uint64]*noncer),
		pendingTxs:        make(map[string]*list),
		txsSigner:         cmap.NewShardedMap(20000),
		mux:               sync.RWMutex{},
	}
	return nsm
}

func (nsm *NamespaceMempool) getPendingTxs(ctx sdk.Context, address string) *list {
	namespaceId := ctx.Namespace()
	addressKey := strconv.FormatUint(namespaceId, 10) + address
	nsm.mux.RLock()
	pendingTxs, init := nsm.pendingTxs[addressKey]
	nsm.mux.RUnlock()
	if !init {
		nsm.mux.Lock()
		pendingTxs, init = nsm.pendingTxs[addressKey]
		if !init {
			pendingTxs = newList()
			nsm.pendingTxs[addressKey] = pendingTxs
		}
		nsm.mux.Unlock()
	}
	return pendingTxs
}

func (nsm *NamespaceMempool) getCheckNoncer(ctx sdk.Context, fallback sdk.GetAccountNonceHandler) *noncer {
	namespaceId := ctx.Namespace()
	nsm.mux.RLock()
	pendingNoncer, init := nsm.signersCheckNonce[namespaceId]
	nsm.mux.RUnlock()
	if !init {
		nsm.mux.Lock()
		pendingNoncer, init = nsm.signersCheckNonce[namespaceId]
		if !init {
			pendingNoncer = newNoncer(fallback)
			nsm.signersCheckNonce[namespaceId] = pendingNoncer
		}
		nsm.mux.Unlock()
	}
	return pendingNoncer
}

func (nsm *NamespaceMempool) getFinalNoncer(ctx sdk.Context, fallback sdk.GetAccountNonceHandler) *noncer {
	namespaceId := ctx.Namespace()
	nsm.mux.RLock()
	finalNoncer, init := nsm.signersFinalNonce[namespaceId]
	nsm.mux.RUnlock()
	if !init {
		nsm.mux.Lock()
		finalNoncer, init = nsm.signersFinalNonce[namespaceId]
		if !init {
			finalNoncer = newNoncer(fallback)
			nsm.signersFinalNonce[namespaceId] = finalNoncer
		}
		nsm.mux.Unlock()
	}
	return finalNoncer
}

// IsGapped reports whether the given transaction is immediately executable.
func (nsm *NamespaceMempool) IsGapped(ctx sdk.Context, tx sdk.MempoolTx) bool {
	pendingNoncer := nsm.getCheckNoncer(ctx, tx.GetAccountNonceHandler)
	finalNoncer := nsm.getFinalNoncer(ctx, func(ctx sdk.Context, address string) (uint64, error) {
		return 0, nil
	})
	maxNonce := pendingNoncer.get(ctx, tx.From)
	minNonce := finalNoncer.get(ctx, tx.From)
	txNonce := tx.Nonce

	println("IsGapped->maxNonce=", maxNonce, ", minNonce=", minNonce, "txNonce=", txNonce)
	if minNonce == 0 {
		return txNonce < maxNonce && txNonce >= minNonce
	}
	return txNonce == minNonce
}
func (nsm *NamespaceMempool) Reset() {
	nsm.mux.Lock()
	defer nsm.mux.Unlock()
	nsm.signersCheckNonce = make(map[uint64]*noncer)
	nsm.signersFinalNonce = make(map[uint64]*noncer)
	nsm.pendingTxs = make(map[string]*list)
	nsm.txsSigner.Clear()
}

func (nsm *NamespaceMempool) Prepare(ctx sdk.Context, tx sdk.MempoolTx) sdk.Context {
	if ctx.ExecMode() == sdk.ExecModeFinalize {
		//set signer
		if signer, ok := nsm.txsSigner.Load(tx.Hash()); ok {
			ctx = ctx.WithTxSigner(signer.(*string))
		}
	}
	return ctx
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.MempoolTx) (context.Context, error) {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	//update nonce
	pendingNoncer := nsm.getCheckNoncer(sdkCtx, tx.GetAccountNonceHandler)
	sender := tx.From
	nonce := sdkCtx.Nonce()

	newNonce := pendingNoncer.setIfHigher(sender, nonce)
	println("=======check set nonce = ", newNonce, "sender=", sender)
	//add signer cache
	txSigner := sdkCtx.TxSigner()
	if txSigner != nil {
		nsm.txsSigner.Store(tx.Hash(), txSigner, func() {})
	}
	//add pending tx
	pendingTxs := nsm.getPendingTxs(sdkCtx, tx.From)
	if pendingTxs.Contains(tx.Nonce) {
		ok, old := pendingTxs.Add(&tx)
		if !ok {
			return sdkCtx, errors.New("replacement transaction underpriced")
		}
		return sdkCtx.WithReplaceTxBytes(old.TxBytes), nil
	} else {
		pendingTxs.Put(&tx)
	}
	return sdkCtx, nil
}
func (nsm *NamespaceMempool) Remove(ctx context.Context, tx sdk.MempoolTx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	finalNoncer := nsm.getFinalNoncer(sdkCtx, func(ctx sdk.Context, address string) (uint64, error) {
		return 0, nil
	})
	sender := tx.From
	nonce := sdkCtx.Nonce()
	newNonce := finalNoncer.setIfHigher(sender, nonce)

	println("=======final set nonce = ", newNonce, "sender=", sender)

	//remove pending tx
	pendingTxs := nsm.getPendingTxs(sdkCtx, tx.From)
	pendingTxs.Remove(&tx)
	return nil
}
func (nsm *NamespaceMempool) Select(context.Context, []sdk.MempoolTx) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	panic("implement me")
}
