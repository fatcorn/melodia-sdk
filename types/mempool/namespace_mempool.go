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
	pending *namespaceList
	signers *cmap.ShardedMap
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		noncer:  newNamepsaceNoncer(),
		pending: newNamepaceList(),
		signers: cmap.NewShardedMap(20000),
	}
	return nsm
}

// IsGapped reports whether the given transaction is immediately executable.
func (nsm *NamespaceMempool) IsGapped(ctx sdk.Context, tx sdk.MempoolTx) bool {
	nextNonce := nsm.noncer.getNonce(ctx, tx.From)
	txNonce := tx.Nonce

	println("IsGapped-> nextNonce=", nextNonce, "txNonce=", txNonce)
	if nextNonce == 0 {
		return txNonce >= nextNonce
	}
	return txNonce >= nextNonce
	//return txNonce == nextNonce
}
func (nsm *NamespaceMempool) Reset() {
	nsm.noncer = newNamepsaceNoncer()
	nsm.pending = newNamepaceList()
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

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.MempoolTx) (context.Context, error) {
	sdkCtx := sdk.UnwrapSDKContext(ctx)

	//add signer cache
	txSigner := sdkCtx.TxSigner()
	if txSigner != nil {
		nsm.signers.Store(tx.Hash(), txSigner, func() {})
	}
	//add pending tx
	oldTx, err := nsm.pending.addTx(sdkCtx, tx)
	if err != nil {
		return nil, err
	}
	if oldTx != nil {
		sdkCtx = sdkCtx.WithReplaceTxBytes(oldTx.TxBytes)
	}
	return sdkCtx, nil
}
func (nsm *NamespaceMempool) Remove(ctx context.Context, tx sdk.MempoolTx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	//update nonce
	nsm.noncer.updateNonce(sdkCtx, tx.From, sdkCtx.Nonce())
	//remove pending tx
	nsm.pending.removeTx(sdkCtx, tx)
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
