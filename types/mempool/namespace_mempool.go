package mempool

import (
	"context"
	"github.com/cometbft/cometbft/libs/cmap"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

var _ Mempool = (*NamespaceMempool)(nil)

// NamespaceMempool defines a namespace sort mempool.
type NamespaceMempool struct {
	noncer       *namepaceNoncer
	cacheSigners *cmap.ShardedMap
	//pendingNonces *cmap.ShardedMap
}

// NewNamespaceMempool creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceMempool() *NamespaceMempool {
	nsm := &NamespaceMempool{
		noncer:       newNamepsaceNoncer(),
		cacheSigners: cmap.NewShardedMap(20000),
		//pendingNonces: cmap.NewShardedMap(20000),
	}
	return nsm
}

// IsGapped reports whether the given transaction is immediately executable.
func (nsm *NamespaceMempool) IsGapped(ctx sdk.Context, tx sdk.MempoolTx) (bool, bool) {
	txNonce := tx.Nonce
	nextNonce := nsm.noncer.getNonce(ctx, tx.From, tx.GetAccountNonceHandler)
	if txNonce != nextNonce {
		println("IsGapped-> nextNonce=", nextNonce, "txNonce=", txNonce, "sender=", tx.From)
	}
	return txNonce == nextNonce, txNonce < nextNonce
}
func (nsm *NamespaceMempool) Reset() {
	nsm.noncer = newNamepsaceNoncer()
	nsm.cacheSigners.Clear()
	//nsm.pendingNonces.Clear()
}

func (nsm *NamespaceMempool) Prepare(ctx sdk.Context, tx sdk.MempoolTx) sdk.Context {
	if ctx.ExecMode() == sdk.ExecModeFinalize {
		//set signer
		if signer, ok := nsm.cacheSigners.Load(tx.Hash()); ok {
			ctx = ctx.WithTxSigner(signer.(*string))
		}
	}
	return ctx
}

func (nsm *NamespaceMempool) Defer(ctx context.Context, tx sdk.MempoolTx, success bool) {
	//update nonce
	if success {
		sdkCtx := sdk.UnwrapSDKContext(ctx)
		if sdkCtx.ExecMode() == sdk.ExecModeFinalize {
			nsm.noncer.updateNonce(sdkCtx, tx.From, sdkCtx.Nonce(), tx.GetAccountNonceHandler)
		}
	}
}

func (nsm *NamespaceMempool) Insert(ctx context.Context, tx sdk.MempoolTx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)

	// If there's an older better MempoolTx, abort
	//oldGasPrice, ok := nsm.pendingNonces.Load(tx.NonceKey(sdkCtx.Namespace()))
	//if ok {
	//	if oldGasPrice.(*big.Int).Cmp(tx.GasPrice) >= 0 {
	//		return errors.New("replacement transaction underpriced")
	//	}
	//}
	//nsm.pendingNonces.Store(tx.NonceKey(sdkCtx.Namespace()), tx.GasPrice, func() {})

	//add signer cache
	txSigner := sdkCtx.TxSigner()
	if txSigner != nil {
		nsm.cacheSigners.Store(tx.Hash(), txSigner, func() {})
	}
	return nil
}
func (nsm *NamespaceMempool) Remove(ctx context.Context, tx sdk.MempoolTx) error {
	//sdkCtx := sdk.UnwrapSDKContext(ctx)
	//remove cache signer
	nsm.cacheSigners.Delete(tx.Hash(), func(any any) {})
	//remove pending nonce
	//nsm.pendingNonces.Delete(tx.NonceKey(sdkCtx.Namespace()), func(any any) {})
	return nil
}
func (nsm *NamespaceMempool) Select(context.Context, []sdk.MempoolTx) Iterator {
	panic("implement me")
}
func (nsm *NamespaceMempool) CountTx() int {
	panic("implement me")
}
