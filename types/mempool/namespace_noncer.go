package mempool

import (
	"context"
	"errors"
	"fmt"
	tmbytes "github.com/cometbft/cometbft/libs/bytes"
	"github.com/cometbft/cometbft/libs/cmap"
	tmtypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	errors2 "github.com/cosmos/cosmos-sdk/types/errors"
	"sync"
)

var _ Mempool = (*NamespaceNoncer)(nil)

// NamespaceNoncer defines a namespace sort mempool.
type NamespaceNoncer struct {
	mtx             sync.RWMutex
	pendingNonces   map[uint64]*noncer // Pending state tracking virtual nonces
	signerExtractor SignerExtractionAdapter
	signerMap       *cmap.ShardedMap
}

// NewNamespaceNoncer creates a new mempool that prioritizes transactions by
// nonce, the lowest first, picking a random sender on each iteration.
func NewNamespaceNoncer() *NamespaceNoncer {
	nsm := &NamespaceNoncer{
		pendingNonces:   make(map[uint64]*noncer),
		signerExtractor: NewDefaultSignerExtractionAdapter(),
	}
	return nsm
}

func (nsm *NamespaceNoncer) UpdateNonce(ctx sdk.Context, tx sdk.Tx, fallback sdk.GetAccountNonceHandler) error {

	pendingNoncer := nsm.getNoncer(ctx, fallback)
	sigs, err := nsm.signerExtractor.GetSigners(tx)
	if err != nil {
		return err
	}
	if len(sigs) == 0 {
		return fmt.Errorf("tx must have at least one signer")
	}

	sig := sigs[0]
	sender := sig.Signer.String()
	nonce := ctx.MaxNonce()
	ctx.Logger().Debug("noncer update", "address", sender, "nonce", pendingNoncer.get(ctx, sender), "newNonce", nonce, "height", ctx.BlockHeight())
	pendingNoncer.setIfHigher(ctx, sender, nonce)

	return nil
}

func (nsm *NamespaceNoncer) GetNonce(ctx sdk.Context, tx sdk.Tx, fallback sdk.GetAccountNonceHandler) (uint64, error) {
	pendingNoncer := nsm.getNoncer(ctx, fallback)

	sigs, err := nsm.signerExtractor.GetSigners(tx)
	if err != nil {
		return 0, err
	}
	if len(sigs) == 0 {
		return 0, fmt.Errorf("tx must have at least one signer")
	}

	sig := sigs[0]
	sender := sig.Signer.String()
	nonce := pendingNoncer.get(ctx, sender)
	return nonce, nil
}

func (nsm *NamespaceNoncer) getNoncer(ctx sdk.Context, fallback sdk.GetAccountNonceHandler) *noncer {
	namespaceId := ctx.Namespace()
	nsm.mtx.RLock()
	pendingNoncer, init := nsm.pendingNonces[namespaceId]
	nsm.mtx.RUnlock()
	if !init {
		nsm.mtx.Lock()
		defer nsm.mtx.Unlock()
		pendingNoncer = newNoncer(fallback)
		nsm.pendingNonces[namespaceId] = pendingNoncer
	}
	return pendingNoncer
}

func (nsm *NamespaceNoncer) Reset() {
	nsm.mtx.Lock()
	defer nsm.mtx.Unlock()
	for _, pn := range nsm.pendingNonces {
		pn.Reset()
	}
}

func (nsm *NamespaceNoncer) GetSigner(txBytes []byte) *string {
	txHash := tmbytes.HexBytes(tmtypes.Tx(txBytes).Hash()).String()
	signer, ok := nsm.signerMap.Load(txHash)
	if ok {
		return signer.(*string)
	}
	return nil
}
func (nsm *NamespaceNoncer) Insert(ctx context.Context, tx sdk.Tx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	txHash := tmbytes.HexBytes(tmtypes.Tx(sdkCtx.TxBytes()).Hash()).String()
	txSigner := sdkCtx.TxSigner()
	if txSigner != nil {
		nsm.signerMap.Store(txHash, txSigner, func() {})
	}
	return nil
}
func (nsm *NamespaceNoncer) Select(context.Context, [][]byte) Iterator {
	panic("implement me")
}
func (nsm *NamespaceNoncer) CountTx() int {
	panic("implement me")
}
func (nsm *NamespaceNoncer) Remove(tx sdk.Tx) error {
	return nil
}

func (nsm *NamespaceNoncer) RemoveWithCtx(ctx context.Context, tx sdk.Tx) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	txHash := tmbytes.HexBytes(tmtypes.Tx(sdkCtx.TxBytes()).Hash()).String()
	nsm.signerMap.Delete(txHash, func(any any) {})
	return nil
}
func (nsm *NamespaceNoncer) Rollback(ctx sdk.Context, tx sdk.Tx, fallback sdk.GetAccountNonceHandler, failReason error) {

	pendingNoncer := nsm.getNoncer(ctx, fallback)

	sigs, err := nsm.signerExtractor.GetSigners(tx)
	if err != nil {
		panic(err)
	}
	if len(sigs) == 0 {
		panic(err)
	}

	sig := sigs[0]
	sender := sig.Signer.String()
	nonce := sig.Sequence
	if errors.Is(failReason, errors2.ErrInvalidSequence) {
		if nonce > ctx.MaxNonce() {
			return
		}
	}

	if nonce > 0 {
		nonce = nonce - 1
	}

	ctx.Logger().Debug("noncer rollback", "address", sender, "nonce", pendingNoncer.get(ctx, sender), "newNonce", nonce, "txNonce", sig.Sequence, "mode", ctx.ExecMode(), "height", ctx.BlockHeight(), "err", failReason)

	pendingNoncer.setIfLower(ctx, sender, nonce)
}
