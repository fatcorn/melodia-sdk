package mempool

import (
	"context"
	"errors"
	"github.com/cometbft/cometbft/libs/cmap"
	sdk "github.com/cosmos/cosmos-sdk/types"
	errors2 "github.com/cosmos/cosmos-sdk/types/errors"
	"sync"
)

var _ Mempool = (*NamespaceMempool)(nil)

// NamespaceMempool defines a namespace sort mempool.
type NamespaceMempool struct {
	signersNonce *sync.Map
	mux          sync.RWMutex
	txsSigner    *cmap.ShardedMap
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		signersNonce: &sync.Map{},
		txsSigner:    cmap.NewShardedMap(20000),
		mux:          sync.RWMutex{},
	}
	return nsm
}

func (nsm *NamespaceMempool) BeforeRun(ctx sdk.Context, tx sdk.MempoolTx, fallback sdk.GetAccountNonceHandler) sdk.Context {
	if ctx.ExecMode() == sdk.ExecModeFinalize {
		//set signer
		if signer, ok := nsm.txsSigner.Load(tx.Hash()); ok {
			ctx = ctx.WithTxSigner(signer.(*string))
		}
		//set max nonce
		maxNonce := nsm.getNoncer(ctx, fallback).get(ctx, tx.From)
		return ctx.WithMaxNonce(maxNonce)
	}
	return ctx
}

func (nsm *NamespaceMempool) AfterRun(ctx sdk.Context, tx sdk.MempoolTx, success bool, fallback sdk.GetAccountNonceHandler) {

	if ctx.ExecMode() == sdk.ExecModeCheck || ctx.ExecMode() == sdk.ExecModeReCheck {
		if success {
			//update nonce
			pendingNoncer := nsm.getNoncer(ctx, fallback)
			sender := tx.From
			nonce := ctx.MaxNonce()
			pendingNoncer.setIfHigher(ctx, sender, nonce)

			//add signer cache
			txSigner := ctx.TxSigner()
			if txSigner != nil {
				nsm.txsSigner.Store(tx.Hash(), txSigner, func() {})
			}
		}
	}
}
func (nsm *NamespaceMempool) DeferRun(ctx sdk.Context, tx sdk.MempoolTx, fallback sdk.GetAccountNonceHandler, failReason error) {
	if failReason != nil {
		nonce := tx.Nonce
		if errors.Is(failReason, errors2.ErrInvalidSequence) {
			if nonce > ctx.MaxNonce() {
				return
			}
		}
		pendingNoncer := nsm.getNoncer(ctx, fallback)
		if nonce > 0 {
			nonce = nonce - 1
		}
		sender := tx.From
		ctx.Logger().Debug("noncer rollback", "address", sender, "nonce", pendingNoncer.get(ctx, sender), "newNonce", nonce, "txNonce", tx.Nonce, "mode", ctx.ExecMode(), "height", ctx.BlockHeight(), "err", failReason)
		pendingNoncer.setIfLower(ctx, sender, nonce)
	}
}

func (nsm *NamespaceMempool) getNoncer(ctx sdk.Context, fallback sdk.GetAccountNonceHandler) *noncer {
	namespaceId := ctx.Namespace()
	pendingNoncer, init := nsm.signersNonce.Load(namespaceId)
	if !init {
		nsm.mux.Lock()
		pendingNoncer, init = nsm.signersNonce.Load(namespaceId)
		if !init {
			pendingNoncer = newNoncer(fallback)
			nsm.signersNonce.Store(namespaceId, pendingNoncer)
		}
		nsm.mux.Unlock()
	}
	return pendingNoncer.(*noncer)
}

func (nsm *NamespaceMempool) Reset() {
	nsm.mux.Lock()
	defer nsm.mux.Unlock()
	nsm.signersNonce = &sync.Map{}
	nsm.txsSigner.Clear()
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.MempoolTx) error {
	return nil
}
func (nsm *NamespaceMempool) Remove(tx sdk.MempoolTx) error {
	return nil
}
func (nsm *NamespaceMempool) Select(context.Context, []sdk.MempoolTx) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	panic("implement me")
}
