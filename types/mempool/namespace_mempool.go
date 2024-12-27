package mempool

import (
	"context"
	"github.com/cometbft/cometbft/libs/cmap"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

var _ Mempool = (*NamespaceMempool)(nil)

// NamespaceMempool defines a namespace sort mempool.
type NamespaceMempool struct {
	noncer  *namepaceNoncer
	signers *cmap.ShardedMap
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		noncer:  newNamepsaceNoncer(),
		signers: cmap.NewShardedMap(20000),
	}
	return nsm
}

// IsGapped reports whether the given transaction is immediately executable.
func (nsm *NamespaceMempool) IsGapped(ctx sdk.Context, tx sdk.MempoolTx) bool {
	nextNonce := nsm.noncer.getNonce(ctx, tx.From)
	txNonce := tx.Nonce

	if nextNonce == 0 {
		return txNonce >= nextNonce
	}
	println("IsGapped-> nextNonce=", nextNonce, "txNonce=", txNonce)
	//return txNonce >= nextNonce
	return txNonce == nextNonce
}
func (nsm *NamespaceMempool) Reset() {
	nsm.noncer = newNamepsaceNoncer()
	nsm.signers.Clear()
}

func (nsm *NamespaceMempool) Prepare(ctx sdk.Context, tx sdk.MempoolTx) sdk.Context {
	if ctx.ExecMode() == sdk.ExecModeFinalize {
		//set signer
		if signer, ok := nsm.signers.Load(tx.Hash()); ok {
			ctx = ctx.WithTxSigner(signer.(*string))
		}
	}
	return ctx
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.MempoolTx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)

	//add signer cache
	txSigner := sdkCtx.TxSigner()
	if txSigner != nil {
		nsm.signers.Store(tx.Hash(), txSigner, func() {})
	}
	return nil
}
func (nsm *NamespaceMempool) Remove(ctx context.Context, tx sdk.MempoolTx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	//update nonce
	nextNonce := nsm.noncer.updateNonce(sdkCtx, tx.From, sdkCtx.Nonce())
	println("next nonce ====", nextNonce, "from=", tx.From)
	//remove cache signer
	nsm.signers.Delete(tx.Hash(), func(any any) {})
	return nil
}
func (nsm *NamespaceMempool) Select(context.Context, []sdk.MempoolTx) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	panic("implement me")
}
