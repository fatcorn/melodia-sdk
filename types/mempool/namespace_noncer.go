package mempool

import (
	"context"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"sync"
)

var _ Mempool = (*NamespaceNoncer)(nil)

// NamespaceNoncer defines a namespace sort mempool.
type NamespaceNoncer struct {
	mtx             sync.RWMutex
	pendingNonces   map[uint64]*noncer // Pending state tracking virtual nonces
	signerExtractor SignerExtractionAdapter
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

func (nsm *NamespaceNoncer) Refresh(ctx sdk.Context, tx sdk.Tx, fallback sdk.GetAccountNonceHandler) error {
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

	pendingNoncer.refresh(ctx, sender)

	//nonce := pendingNoncer.get(ctx, sender)

	//// we merged the nonce verification to nonce increment, so when tx includes multiple messages
	//// with same sender, they'll be accepted.
	//if txNonce != nonce {
	//	nonce = pendingNoncer.getLatest(ctx, sender)
	//	if txNonce != nonce {
	//		return errorsmod.Wrapf(
	//			errors.ErrInvalidSequence,
	//			"invalid signer nonce; got %d, expected %d", txNonce, nonce,
	//		)
	//	}
	//}

	//nonce++
	//pendingNoncer.set(ctx, sender, nonce)
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
		clean := make(map[string]uint64)
		pn.setAll(clean)
	}
}

func (nsm *NamespaceNoncer) Insert(context.Context, sdk.Tx) error {
	panic("implement me")
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
	namespaceId := sdkCtx.Namespace()
	nsm.mtx.RLock()
	pendingNoncer, init := nsm.pendingNonces[namespaceId]
	if !init {
		return nil
	}
	nsm.mtx.RUnlock()

	sigs, err := nsm.signerExtractor.GetSigners(tx)
	if err != nil {
		return err
	}
	if len(sigs) == 0 {
		return fmt.Errorf("tx must have at least one signer")
	}

	sig := sigs[0]
	sender := sig.Signer.String()
	nonce := sig.Sequence

	pendingNoncer.setIfLower(sdkCtx, sender, nonce)
	return nil
}
