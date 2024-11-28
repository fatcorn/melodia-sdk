package baseapp

import (
	"bytes"
	"context"
	"fmt"
	tmbytes "github.com/cometbft/cometbft/libs/bytes"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	abci "github.com/cometbft/cometbft/abci/types"
	cryptoenc "github.com/cometbft/cometbft/crypto/encoding"
	cmtprotocrypto "github.com/cometbft/cometbft/proto/tendermint/crypto"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	consensustypes "github.com/cometbft/cometbft/types"
	protoio "github.com/cosmos/gogoproto/io"
	"github.com/cosmos/gogoproto/proto"

	"cosmossdk.io/core/comet"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/mempool"
)

type (
	// ValidatorStore defines the interface contract require for verifying vote
	// extension signatures. Typically, this will be implemented by the x/staking
	// module, which has knowledge of the CometBFT public key.
	ValidatorStore interface {
		GetPubKeyByConsAddr(context.Context, sdk.ConsAddress) (cmtprotocrypto.PublicKey, error)
	}

	// GasTx defines the contract that a transaction with a gas limit must implement.
	GasTx interface {
		GetGas() uint64
	}
)

// ValidateVoteExtensions defines a helper function for verifying vote extension
// signatures that may be passed or manually injected into a block proposal from
// a proposer in PrepareProposal. It returns an error if any signature is invalid
// or if unexpected vote extensions and/or signatures are found or less than 2/3
// power is received.
// NOTE: From v0.50.5 `currentHeight` and `chainID` arguments are ignored for fixing an issue.
// They will be removed from the function in v0.51+.
func ValidateVoteExtensions(
	ctx sdk.Context,
	valStore ValidatorStore,
	_ int64,
	_ string,
	extCommit abci.ExtendedCommitInfo,
) error {
	// Get values from context
	cp := ctx.ConsensusParams()
	currentHeight := ctx.HeaderInfo().Height
	chainID := ctx.HeaderInfo().ChainID
	commitInfo := ctx.CometInfo().GetLastCommit()

	// Check that both extCommit + commit are ordered in accordance with vp/address.
	if err := validateExtendedCommitAgainstLastCommit(extCommit, commitInfo); err != nil {
		return err
	}

	// Start checking vote extensions only **after** the vote extensions enable
	// height, because when `currentHeight == VoteExtensionsEnableHeight`
	// PrepareProposal doesn't get any vote extensions in its request.
	extsEnabled := cp.Abci != nil && currentHeight > cp.Abci.VoteExtensionsEnableHeight && cp.Abci.VoteExtensionsEnableHeight != 0
	marshalDelimitedFn := func(msg proto.Message) ([]byte, error) {
		var buf bytes.Buffer
		if err := protoio.NewDelimitedWriter(&buf).WriteMsg(msg); err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	var (
		// Total voting power of all vote extensions.
		totalVP int64
		// Total voting power of all validators that submitted valid vote extensions.
		sumVP int64
	)

	for _, vote := range extCommit.Votes {
		totalVP += vote.Validator.Power

		// Only check + include power if the vote is a commit vote. There must be super-majority, otherwise the
		// previous block (the block vote is for) could not have been committed.
		if vote.BlockIdFlag != cmtproto.BlockIDFlagCommit {
			continue
		}

		if !extsEnabled {
			if len(vote.VoteExtension) > 0 {
				return fmt.Errorf("vote extensions disabled; received non-empty vote extension at height %d", currentHeight)
			}
			if len(vote.ExtensionSignature) > 0 {
				return fmt.Errorf("vote extensions disabled; received non-empty vote extension signature at height %d", currentHeight)
			}

			continue
		}

		if len(vote.ExtensionSignature) == 0 {
			return fmt.Errorf("vote extensions enabled; received empty vote extension signature at height %d", currentHeight)
		}

		valConsAddr := sdk.ConsAddress(vote.Validator.Address)

		pubKeyProto, err := valStore.GetPubKeyByConsAddr(ctx, valConsAddr)
		if err != nil {
			return fmt.Errorf("failed to get validator %X public key: %w", valConsAddr, err)
		}

		cmtPubKey, err := cryptoenc.PubKeyFromProto(pubKeyProto)
		if err != nil {
			return fmt.Errorf("failed to convert validator %X public key: %w", valConsAddr, err)
		}

		cve := cmtproto.CanonicalVoteExtension{
			Extension: vote.VoteExtension,
			Height:    currentHeight - 1, // the vote extension was signed in the previous height
			Round:     int64(extCommit.Round),
			ChainId:   chainID,
		}

		extSignBytes, err := marshalDelimitedFn(&cve)
		if err != nil {
			return fmt.Errorf("failed to encode CanonicalVoteExtension: %w", err)
		}

		if !cmtPubKey.VerifySignature(extSignBytes, vote.ExtensionSignature) {
			return fmt.Errorf("failed to verify validator %X vote extension signature", valConsAddr)
		}

		sumVP += vote.Validator.Power
	}

	// This check is probably unnecessary, but better safe than sorry.
	if totalVP <= 0 {
		return fmt.Errorf("total voting power must be positive, got: %d", totalVP)
	}

	// If the sum of the voting power has not reached (2/3 + 1) we need to error.
	if requiredVP := ((totalVP * 2) / 3) + 1; sumVP < requiredVP {
		return fmt.Errorf(
			"insufficient cumulative voting power received to verify vote extensions; got: %d, expected: >=%d",
			sumVP, requiredVP,
		)
	}
	return nil
}

// validateExtendedCommitAgainstLastCommit validates an ExtendedCommitInfo against a LastCommit. Specifically,
// it checks that the ExtendedCommit + LastCommit (for the same height), are consistent with each other + that
// they are ordered correctly (by voting power) in accordance with
// [comet](https://github.com/cometbft/cometbft/blob/4ce0277b35f31985bbf2c25d3806a184a4510010/types/validator_set.go#L784).
func validateExtendedCommitAgainstLastCommit(ec abci.ExtendedCommitInfo, lc comet.CommitInfo) error {
	// check that the rounds are the same
	if ec.Round != lc.Round() {
		return fmt.Errorf("extended commit round %d does not match last commit round %d", ec.Round, lc.Round())
	}

	// check that the # of votes are the same
	if len(ec.Votes) != lc.Votes().Len() {
		return fmt.Errorf("extended commit votes length %d does not match last commit votes length %d", len(ec.Votes), lc.Votes().Len())
	}

	// check sort order of extended commit votes
	if !slices.IsSortedFunc(ec.Votes, func(vote1, vote2 abci.ExtendedVoteInfo) int {
		if vote1.Validator.Power == vote2.Validator.Power {
			return bytes.Compare(vote1.Validator.Address, vote2.Validator.Address) // addresses sorted in ascending order (used to break vp conflicts)
		}
		return -int(vote1.Validator.Power - vote2.Validator.Power) // vp sorted in descending order
	}) {
		return fmt.Errorf("extended commit votes are not sorted by voting power")
	}

	addressCache := make(map[string]struct{}, len(ec.Votes))
	// check that consistency between LastCommit and ExtendedCommit
	for i, vote := range ec.Votes {
		// cache addresses to check for duplicates
		if _, ok := addressCache[string(vote.Validator.Address)]; ok {
			return fmt.Errorf("extended commit vote address %X is duplicated", vote.Validator.Address)
		}
		addressCache[string(vote.Validator.Address)] = struct{}{}

		if !bytes.Equal(vote.Validator.Address, lc.Votes().Get(i).Validator().Address()) {
			return fmt.Errorf("extended commit vote address %X does not match last commit vote address %X", vote.Validator.Address, lc.Votes().Get(i).Validator().Address())
		}
		if vote.Validator.Power != lc.Votes().Get(i).Validator().Power() {
			return fmt.Errorf("extended commit vote power %d does not match last commit vote power %d", vote.Validator.Power, lc.Votes().Get(i).Validator().Power())
		}
	}

	return nil
}

type (
	// ProposalTxVerifier defines the interface that is implemented by BaseApp,
	// that any custom ABCI PrepareProposal and ProcessProposal handler can use
	// to verify a transaction.
	ProposalTxVerifier interface {
		PrepareProposalVerifyTx(tx sdk.Tx) ([]byte, error)
		ProcessProposalVerifyTx(txBz []byte) (sdk.Tx, error)
		TxDecode(txBz []byte) (sdk.Tx, error)
		TxEncode(tx sdk.Tx) ([]byte, error)
	}

	// DefaultProposalHandler defines the default ABCI PrepareProposal and
	// ProcessProposal handlers.
	DefaultProposalHandler struct {
		mempool          mempool.Mempool
		txVerifier       ProposalTxVerifier
		txSelector       TxSelector
		signerExtAdapter mempool.SignerExtractionAdapter
	}
)

func NewDefaultProposalHandler(mp mempool.Mempool, txVerifier ProposalTxVerifier) *DefaultProposalHandler {
	return &DefaultProposalHandler{
		mempool:          mp,
		txVerifier:       txVerifier,
		txSelector:       NewDefaultTxSelector(),
		signerExtAdapter: mempool.NewDefaultSignerExtractionAdapter(),
	}
}

// SetTxSelector sets the TxSelector function on the DefaultProposalHandler.
func (h *DefaultProposalHandler) SetTxSelector(ts TxSelector) {
	h.txSelector = ts
}

// PrepareProposalHandler returns the default implementation for processing an
// ABCI proposal. The application's mempool is enumerated and all valid
// transactions are added to the proposal. Transactions are valid if they:
//
// 1) Successfully encode to bytes.
// 2) Are valid (i.e. pass runTx, AnteHandler only).
//
// Enumeration is halted once RequestPrepareProposal.MaxBytes of transactions is
// reached or the mempool is exhausted.
//
// Note:
//
// - Step (2) is identical to the validation step performed in
// DefaultProcessProposal. It is very important that the same validation logic
// is used in both steps, and applications must ensure that this is the case in
// non-default handlers.
//
// - If no mempool is set or if the mempool is a no-op mempool, the transactions
// requested from CometBFT will simply be returned, which, by default, are in
// FIFO order.
func (h *DefaultProposalHandler) PrepareProposalHandler() sdk.PrepareProposalHandler {
	return func(ctx sdk.Context, req *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error) {
		var maxBlockGas uint64
		if b := ctx.ConsensusParams().Block; b != nil {
			maxBlockGas = uint64(b.MaxGas)
		}

		defer h.txSelector.Clear()

		// If the mempool is nil or NoOp we simply return the transactions
		// requested from CometBFT, which, by default, should be in FIFO order.
		//
		// Note, we still need to ensure the transactions returned respect req.MaxTxBytes.
		isDef := false
		switch h.mempool.(type) {
		case *mempool.NamespaceMempool:
			isDef = true
		case *mempool.NamespaceNoncer:
			isDef = true
		case mempool.NoOpMempool:
			isDef = true
		}

		if h.mempool == nil || isDef {

			for _, txBz := range req.Txs {
				tx, err := h.txVerifier.TxDecode(txBz)
				if err != nil {
					return nil, err
				}

				stop := h.txSelector.SelectTxForProposal(ctx, uint64(req.MaxTxBytes), maxBlockGas, tx, txBz)
				if stop {
					break
				}
			}
			newTxs := h.txSelector.SelectedTxs(ctx)
			newNsTable := req.NsTable
			newOffset := len(newTxs)
			if newOffset != len(req.Txs) {
				newNsTable = make([]*cmtproto.NamespaceTableEntries, 0)
				for _, nsEntry := range req.NsTable {
					if int(nsEntry.Offset) >= newOffset {
						nsEntry.Offset = int32(newOffset)
						newNsTable = append(newNsTable, nsEntry)
						break
					}
					newNsTable = append(newNsTable, nsEntry)
				}
			}
			return &abci.ResponsePrepareProposal{Txs: newTxs, NsTable: newNsTable}, nil
		}

		iterator := h.mempool.Select(ctx, req.Txs)
		selectedTxsSignersSeqs := make(map[string]uint64)
		var selectedTxsNums int

		for iterator != nil {
			memTx := iterator.Tx()
			signerData, err := h.signerExtAdapter.GetSigners(memTx)
			if err != nil {
				return nil, err
			}

			// If the signers aren't in selectedTxsSignersSeqs then we haven't seen them before
			// so we add them and continue given that we don't need to check the sequence.
			shouldAdd := true
			txSignersSeqs := make(map[string]uint64)
			for _, signer := range signerData {
				seq, ok := selectedTxsSignersSeqs[signer.Signer.String()]
				if !ok {
					txSignersSeqs[signer.Signer.String()] = signer.Sequence
					continue
				}

				// If we have seen this signer before in this block, we must make
				// sure that the current sequence is seq+1; otherwise is invalid
				// and we skip it.
				if seq+1 != signer.Sequence {
					shouldAdd = false
					break
				}
				txSignersSeqs[signer.Signer.String()] = signer.Sequence
			}
			if !shouldAdd {
				iterator = iterator.Next()
				continue
			}

			// NOTE: Since transaction verification was already executed in CheckTx,
			// which calls mempool.Insert, in theory everything in the pool should be
			// valid. But some mempool implementations may insert invalid txs, so we
			// check again.

			txBz, err := h.txVerifier.PrepareProposalVerifyTx(memTx)
			if err != nil {
				err := h.mempool.Remove(memTx)
				if err != nil && !errors.Is(err, mempool.ErrTxNotFound) {
					return nil, err
				}
			} else {
				stop := h.txSelector.SelectTxForProposal(ctx, uint64(req.MaxTxBytes), maxBlockGas, memTx, txBz)
				if stop {
					break
				}

				txsLen := len(h.txSelector.SelectedTxs(ctx))
				for sender, seq := range txSignersSeqs {
					// If txsLen != selectedTxsNums is true, it means that we've
					// added a new tx to the selected txs, so we need to update
					// the sequence of the sender.
					if txsLen != selectedTxsNums {
						selectedTxsSignersSeqs[sender] = seq
					} else if _, ok := selectedTxsSignersSeqs[sender]; !ok {
						// The transaction hasn't been added but it passed the
						// verification, so we know that the sequence is correct.
						// So we set this sender's sequence to seq-1, in order
						// to avoid unnecessary calls to PrepareProposalVerifyTx.
						selectedTxsSignersSeqs[sender] = seq - 1
					}
				}
				selectedTxsNums = txsLen
			}

			iterator = iterator.Next()
		}

		return &abci.ResponsePrepareProposal{Txs: h.txSelector.SelectedTxs(ctx)}, nil
	}
}

// ProcessProposalHandler returns the default implementation for processing an
// ABCI proposal. Every transaction in the proposal must pass 2 conditions:
//
// 1. The transaction bytes must decode to a valid transaction.
// 2. The transaction must be valid (i.e. pass runTx, AnteHandler only)
//
// If any transaction fails to pass either condition, the proposal is rejected.
// Note that step (2) is identical to the validation step performed in
// DefaultPrepareProposal. It is very important that the same validation logic
// is used in both steps, and applications must ensure that this is the case in
// non-default handlers.
func (h *DefaultProposalHandler) ProcessProposalHandler() sdk.ProcessProposalHandler {
	// If the mempool is nil or NoOp we simply return ACCEPT,
	// because PrepareProposal may have included txs that could fail verification.
	isDef := false
	switch h.mempool.(type) {
	case *mempool.NamespaceMempool:
		isDef = true
	case *mempool.NamespaceNoncer:
		isDef = true
	case mempool.NoOpMempool:
		isDef = true
	}

	if h.mempool == nil || isDef {
		return NoOpProcessProposal()
	}
	return func(ctx sdk.Context, req *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error) {
		var totalTxGas uint64

		var maxBlockGas int64
		if b := ctx.ConsensusParams().Block; b != nil {
			maxBlockGas = b.MaxGas
		}
		namespaceIdList, err := consensustypes.ListTxsNamespaceIdsWithTxsAndNSTable(len(req.Txs), req.NsTable)
		if err != nil {
			panic("Unmatched txs with tx NS table")
		}
		for i, txBytes := range req.Txs {
			app := h.txVerifier.(*BaseApp)
			app.SetNamespace(execModeProcessProposal, namespaceIdList[i])
			tx, err := h.txVerifier.ProcessProposalVerifyTx(txBytes)
			if err != nil {
				return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
			}

			if maxBlockGas > 0 {
				gasTx, ok := tx.(GasTx)
				if ok {
					totalTxGas += gasTx.GetGas()
				}

				if totalTxGas > uint64(maxBlockGas) {
					return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_REJECT}, nil
				}
			}
		}

		return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_ACCEPT}, nil
	}
}

// NoOpPrepareProposal defines a no-op PrepareProposal handler. It will always
// return the transactions sent by the client's request.
func NoOpPrepareProposal() sdk.PrepareProposalHandler {
	return func(_ sdk.Context, req *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error) {
		return &abci.ResponsePrepareProposal{Txs: req.Txs}, nil
	}
}

// NoOpProcessProposal defines a no-op ProcessProposal Handler. It will always
// return ACCEPT.
func NoOpProcessProposal() sdk.ProcessProposalHandler {
	return func(_ sdk.Context, _ *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error) {
		return &abci.ResponseProcessProposal{Status: abci.ResponseProcessProposal_ACCEPT}, nil
	}
}

// NoOpExtendVote defines a no-op ExtendVote handler. It will always return an
// empty byte slice as the vote extension.
func NoOpExtendVote() sdk.ExtendVoteHandler {
	return func(_ sdk.Context, _ *abci.RequestExtendVote) (*abci.ResponseExtendVote, error) {
		return &abci.ResponseExtendVote{VoteExtension: []byte{}}, nil
	}
}

// NoOpVerifyVoteExtensionHandler defines a no-op VerifyVoteExtension handler. It
// will always return an ACCEPT status with no error.
func NoOpVerifyVoteExtensionHandler() sdk.VerifyVoteExtensionHandler {
	return func(_ sdk.Context, _ *abci.RequestVerifyVoteExtension) (*abci.ResponseVerifyVoteExtension, error) {
		return &abci.ResponseVerifyVoteExtension{Status: abci.ResponseVerifyVoteExtension_ACCEPT}, nil
	}
}

// TxSelector defines a helper type that assists in selecting transactions during
// mempool transaction selection in PrepareProposal. It keeps track of the total
// number of bytes and total gas of the selected transactions. It also keeps
// track of the selected transactions themselves.
type TxSelector interface {
	// SelectedTxs should return a copy of the selected transactions.
	SelectedTxs(ctx context.Context) [][]byte

	// Clear should clear the TxSelector, nulling out all relevant fields.
	Clear()

	// SelectTxForProposal should attempt to select a transaction for inclusion in
	// a proposal based on inclusion criteria defined by the TxSelector. It must
	// return <true> if the caller should halt the transaction selection loop
	// (typically over a mempool) or <false> otherwise.
	SelectTxForProposal(ctx context.Context, maxTxBytes, maxBlockGas uint64, memTx sdk.Tx, txBz []byte) bool
}

type defaultTxSelector struct {
	totalTxBytes uint64
	totalTxGas   uint64
	selectedTxs  [][]byte
}

func NewDefaultTxSelector() TxSelector {
	return &defaultTxSelector{}
}

func (ts *defaultTxSelector) SelectedTxs(_ context.Context) [][]byte {
	txs := make([][]byte, len(ts.selectedTxs))
	copy(txs, ts.selectedTxs)
	return txs
}

func (ts *defaultTxSelector) Clear() {
	ts.totalTxBytes = 0
	ts.totalTxGas = 0
	ts.selectedTxs = nil
}

func (ts *defaultTxSelector) SelectTxForProposal(ctx context.Context, maxTxBytes, maxBlockGas uint64, memTx sdk.Tx, txBz []byte) bool {

	if maxTxBytes == 0 && maxBlockGas == 0 {
		return ts.SelectFullTxForProposal(ctx, memTx, txBz)
	}

	txSize := uint64(len(txBz))

	var txGasLimit uint64
	if memTx != nil {
		if gasTx, ok := memTx.(GasTx); ok {
			txGasLimit = gasTx.GetGas()
		}
	}

	// only add the transaction to the proposal if we have enough capacity
	if (txSize + ts.totalTxBytes) <= maxTxBytes {
		// If there is a max block gas limit, add the tx only if the limit has
		// not been met.
		if maxBlockGas > 0 {
			if (txGasLimit + ts.totalTxGas) <= maxBlockGas {
				ts.totalTxGas += txGasLimit
				ts.totalTxBytes += txSize
				ts.selectedTxs = append(ts.selectedTxs, txBz)
			}
		} else {
			ts.totalTxBytes += txSize
			ts.selectedTxs = append(ts.selectedTxs, txBz)
		}
	}

	// check if we've reached capacity; if so, we cannot select any more transactions
	return ts.totalTxBytes >= maxTxBytes || (maxBlockGas > 0 && (ts.totalTxGas >= maxBlockGas))
}

func (ts *defaultTxSelector) SelectFullTxForProposal(_ context.Context, memTx sdk.Tx, txBz []byte) bool {
	txSize := uint64(len(txBz))
	var txGasLimit uint64
	if memTx != nil {
		if gasTx, ok := memTx.(GasTx); ok {
			txGasLimit = gasTx.GetGas()
		}
	}
	ts.totalTxGas += txGasLimit
	ts.totalTxBytes += txSize
	ts.selectedTxs = append(ts.selectedTxs, txBz)
	return true
}

// SortTxsOrders return sort orders
func (app *BaseApp) SortTxsOrders(ctx sdk.Context, req *abci.RequestSortTxs) (*abci.ResponseSortTxs, error) {
	startTime := time.Now()
	reqByteTxs := make([]sdk.ByteTx, len(req.Txs))
	switch m := app.mempool.(type) {
	case *mempool.NamespaceNoncer:
		for i, txByte := range req.Txs {
			byteTx := m.GetTx(txByte)
			if byteTx == nil {
				tx, err := app.TxDecode(txByte)
				if err != nil {
					continue
				}
				byteTx = &sdk.ByteTx{Tx: tx, TxBytes: txByte}
			}
			reqByteTxs[i] = *byteTx
		}
	default:
		reqByteTxs, _ = app.TxDecodeBatch(req.Txs)
	}
	ctx.Logger().Info("1->sortTxsOrders TxDecodeBatch time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(req.Txs), "height", ctx.BlockHeight())
	var (
		beforeInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
		afterInterChainPairTxs  = make(map[string][]*cmtproto.NsInterChainTransactionIndex)

		listNamespaceByteTxs = func(byteTxs []sdk.ByteTx, nsTable []*cmtproto.NamespaceTableEntries) map[uint64][]sdk.ByteTx {
			nsByteTxs := make(map[uint64][]sdk.ByteTx)
			lastOffset := int32(0)
			for _, entry := range nsTable {
				namespaceTxs := byteTxs[lastOffset:entry.Offset]
				namespaceId := entry.Namespace
				lastOffset = entry.Offset
				nsByteTxs[namespaceId] = namespaceTxs
			}
			return nsByteTxs
		}
	)

	var (
		orders = &cmtproto.Orders{
			NsTransactionOrders: &cmtproto.NsTransactionOrders{
				Entries: make([]*cmtproto.NsTransactionOrderEntry, 0)},
			NsInterChainTransactionOrders: &cmtproto.NsInterChainTransactionOrders{
				Entries: make([]*cmtproto.NsInterChainTransactionOrderEntry, 0),
			},
		}
		txIndex                             = 0
		handleNsInterChainTransactionOrders = func(byteTx sdk.ByteTx, txIdx int, namespaceId uint64) bool {
			tx := byteTx.Tx
			txBz := byteTx.TxBytes
			txIdBz, isCross, isFirst := app.crossNamespaceTransactionIdentifierHandler(ctx.WithTxBytes(txBz), tx)
			if isCross {
				txId := tmbytes.HexBytes(txIdBz).String()
				if pairTxs, inPair := beforeInterChainPairTxs[txId]; inPair {
					if len(pairTxs) != 2 {
						return false
					}
				}

				if interTxs, ok := afterInterChainPairTxs[txId]; ok {
					interTxs = append(interTxs, &cmtproto.NsInterChainTransactionIndex{
						Namespace: namespaceId,
						Index:     int32(txIdx),
					})

					idxes := make([]*cmtproto.NsInterChainTransactionIndex, 2)

					if isFirst {
						idxes[0] = interTxs[1]
						idxes[1] = interTxs[0]
					} else {
						idxes[0] = interTxs[0]
						idxes[1] = interTxs[1]
					}

					entry := &cmtproto.NsInterChainTransactionOrderEntry{
						Indexes: idxes,
					}
					orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
				} else {
					afterInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
						{
							Namespace: namespaceId,
							Index:     int32(txIdx),
						},
					}
				}
			}
			return true
		}
	)
	staticTime := time.Now()
	nsByteTxs := listNamespaceByteTxs(reqByteTxs, req.NsTable)
	for namespaceId, byteTxs := range nsByteTxs {

		var (
			addressIndexMap                 = make(map[string]int)
			lastTxIndexMap                  = make(map[string]int)
			nsTransactionIndexes            = make([]*cmtproto.NsTransactionIndexes, 0, len(byteTxs))
			handleNsTransactionDependOrders = func(txIdx int, namespaceId uint64, from string, to string) {
				if lastToIdx, toExists := lastTxIndexMap[to]; toExists {
					if lastFromIdx, fromExists := lastTxIndexMap[from]; !fromExists || lastFromIdx != lastToIdx {
						entry := &cmtproto.NsInterChainTransactionOrderEntry{
							Indexes: []*cmtproto.NsInterChainTransactionIndex{
								{
									Namespace: namespaceId,
									Index:     int32(lastToIdx),
								},
								{
									Namespace: namespaceId,
									Index:     int32(txIdx),
								},
							},
						}
						orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
					}
				}
				lastTxIndexMap[from] = txIdx
				lastTxIndexMap[to] = txIdx
			}

			handleNsTransactionOrders = func(tx sdk.Tx, txIdx int, namespaceId uint64) {
				var (
					fromIdx    int
					toIdx      int
					fromExists bool
					toExists   bool
					from       string
					to         string
					curIdx     int
					isToSerial bool
					toIsSerial = func(namespaceId uint64, to string) bool {
						isContract, isContractSerial := app.contractStaticAnalysisResultHandler(ctx.WithNamepace(namespaceId), to)
						if isContract {
							return isContractSerial
						}
						return true
					}
				)

				defer func() {
					if isToSerial {
						handleNsTransactionDependOrders(txIdx, namespaceId, from, to)
					}
				}()

				from, to = app.txAccountInfoHandler(tx)

				fromIdx, fromExists = addressIndexMap[from]
				toIdx, toExists = addressIndexMap[to]

				if fromExists && toExists {
					//Both exist,add from index
					curIdx = fromIdx
					nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))

					//Check if they should be depends
					if toIsSerial(namespaceId, to) {
						isToSerial = true
					}
				} else if fromExists {
					// Only from exists, add to index
					curIdx = fromIdx
					nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
					addressIndexMap[to] = curIdx
				} else if toExists {
					// Only to exists, add from index
					if toIsSerial(namespaceId, to) {
						isToSerial = true
						//append to index
						curIdx = toIdx
						nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
						addressIndexMap[from] = curIdx
					} else {
						//create new from index
						nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
						curIdx = len(nsTransactionIndexes) - 1
						addressIndexMap[from] = curIdx
					}
				} else {
					// Neither exists, create both indices
					//create new from & to index
					nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
					curIdx = len(nsTransactionIndexes) - 1
					addressIndexMap[from] = curIdx
					addressIndexMap[to] = curIdx
				}
			}
		)

		orderTime := time.Now()
		for _, byteTx := range byteTxs {
			txBz := byteTx.TxBytes
			tx := byteTx.Tx
			if tx == nil || txBz == nil {
				continue
			}
			ok := handleNsInterChainTransactionOrders(byteTx, txIndex, namespaceId)
			if !ok {
				continue
			}
			handleNsTransactionOrders(tx, txIndex, namespaceId)
			txIndex++
		}
		ctx.Logger().Info("2.1->sortTxsOrders orders time", "used", fmt.Sprintf("%v ms", time.Since(orderTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())

		if len(nsTransactionIndexes) > 0 {
			orders.NsTransactionOrders.Entries = append(orders.NsTransactionOrders.Entries, &cmtproto.NsTransactionOrderEntry{
				Namespace: namespaceId,
				Indexes:   nsTransactionIndexes,
			})
		}

	}
	ctx.Logger().Info("2->sortTxsOrders total orders time", "used", fmt.Sprintf("%v ms", time.Since(staticTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())
	ctx.Logger().Info("3->sortTxsOrders total time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())

	return &abci.ResponseSortTxs{Txs: req.Txs, NsTable: req.NsTable, Orders: orders}, nil
}

// SortTxsNsTable return sort txs and nsTable
func (app *BaseApp) SortTxsNsTable(ctx sdk.Context, req *abci.RequestSortTxs) (*abci.ResponseSortTxs, error) {
	startTime := time.Now()
	reqByteTxs := make([]sdk.ByteTx, len(req.Txs))
	switch m := app.mempool.(type) {
	case *mempool.NamespaceNoncer:
		for i, txByte := range req.Txs {
			byteTx := m.GetTx(txByte)
			if byteTx == nil {
				tx, err := app.TxDecode(txByte)
				if err != nil {
					continue
				}
				byteTx = &sdk.ByteTx{Tx: tx, TxBytes: txByte}
			}
			reqByteTxs[i] = *byteTx
		}
	default:
		reqByteTxs, _ = app.TxDecodeBatch(req.Txs)
	}

	ctx.Logger().Info("1->sortTxsNsTable TxDecodeBatch time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(req.Txs), "height", ctx.BlockHeight())
	insertTime := time.Now()
	var (
		sortTxs   = make([][]byte, 0)
		wg        sync.WaitGroup
		nsMempool = mempool.NewNamespaceMempool()

		listNamespaceByteTxs = func(byteTxs []sdk.ByteTx, nsTable []*cmtproto.NamespaceTableEntries) map[uint64][]sdk.ByteTx {
			nsByteTxs := make(map[uint64][]sdk.ByteTx)
			lastOffset := int32(0)
			for _, entry := range nsTable {
				namespaceTxs := byteTxs[lastOffset:entry.Offset]
				namespaceId := entry.Namespace
				lastOffset = entry.Offset
				nsByteTxs[namespaceId] = namespaceTxs
			}
			return nsByteTxs
		}

		nsMempoolInsertTxs = func(ctx sdk.Context, namespaceTxs []sdk.ByteTx, namespaceId uint64) {
			defer wg.Done()
			singleStartTime := time.Now()
			for _, byteTx := range namespaceTxs {
				tx := byteTx.Tx
				txBz := byteTx.TxBytes
				if tx == nil || txBz == nil {
					continue
				}
				ctx = ctx.WithNamepace(namespaceId)
				_ = nsMempool.Insert(ctx, tx)

			}
			ctx.Logger().Info("2.1->sortTxsNsTable single namespace insert time", "used", fmt.Sprintf("%v ms", time.Since(singleStartTime).Milliseconds()), "size", len(namespaceTxs), "namespaceId", namespaceId, "height", ctx.BlockHeight())
		}
	)
	nsByteTxs := listNamespaceByteTxs(reqByteTxs, req.NsTable)

	for namespaceId, byteTxs := range nsByteTxs {
		wg.Add(1)
		go nsMempoolInsertTxs(ctx, byteTxs, namespaceId)
	}
	wg.Wait()
	ctx.Logger().Info("2->sortTxsNsTable total insert time", "used", fmt.Sprintf("%v ms", time.Since(insertTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "height", ctx.BlockHeight())

	sortTime := time.Now()
	nsIterators := nsMempool.SelectWithNsTable(ctx, req.Txs, req.NsTable)

	ctx.Logger().Info("3->sortTxsNsTable sort time", "used", fmt.Sprintf("%v ms", time.Since(sortTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "height", ctx.BlockHeight())

	var (
		offset  = 0
		nsTable = make([]*cmtproto.NamespaceTableEntries, 0)
		txIndex = 0
	)
	staticTime := time.Now()
	for _, nsIterator := range nsIterators {
		iterator := nsIterator.Iterator
		namespaceId := nsIterator.NamespaceId

		txSize := nsMempool.CountTxByNamespaceId(namespaceId)
		// Static analysis
		var (
			txs     = make([]sdk.Tx, 0, txSize)
			byteTxs = make([]sdk.ByteTx, 0, txSize)
		)

		// If the iterator is nil use the NoOpMempool, get all namespace txs
		if iterator == nil {
			byteTxs = nsByteTxs[namespaceId]
		} else {
			for iterator != nil {
				tx := iterator.Tx()
				txs = append(txs, tx)
				iterator = iterator.Next()
			}
			batchTime := time.Now()
			byteTxs, _ = app.TxEncodeBatch(txs)
			ctx.Logger().Info("4.1->sortTxsNsTable iterator TxEncodeBatch  time", "used", fmt.Sprintf("%v ms", time.Since(batchTime).Milliseconds()), "namespaceId", namespaceId, "height", ctx.BlockHeight())
		}
		for _, byteTx := range byteTxs {
			txBz := byteTx.TxBytes
			tx := byteTx.Tx
			if tx == nil || txBz == nil {
				continue
			}
			sortTxs = append(sortTxs, txBz)
			offset++
			txIndex++
		}
		nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    int32(offset),
		})

	}
	ctx.Logger().Info("4->sortTxsNsTable total nsTable time", "used", fmt.Sprintf("%v ms", time.Since(staticTime).Milliseconds()), "size", len(sortTxs), "nsTable", nsTable, "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())
	ctx.Logger().Info("5->sortTxsNsTable total time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(sortTxs), "nsTable", nsTable, "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())

	return &abci.ResponseSortTxs{Txs: sortTxs, NsTable: nsTable, Orders: nil}, nil
}

// SortTxsAll return sort txs and nsTable & orders
func (app *BaseApp) SortTxsAll(ctx sdk.Context, req *abci.RequestSortTxs) (*abci.ResponseSortTxs, error) {
	startTime := time.Now()
	reqByteTxs := make([]sdk.ByteTx, len(req.Txs))
	switch m := app.mempool.(type) {
	case *mempool.NamespaceNoncer:
		for i, txByte := range req.Txs {
			byteTx := m.GetTx(txByte)
			if byteTx == nil {
				tx, err := app.TxDecode(txByte)
				if err != nil {
					continue
				}
				byteTx = &sdk.ByteTx{Tx: tx, TxBytes: txByte}
			}
			reqByteTxs[i] = *byteTx
		}
	default:
		reqByteTxs, _ = app.TxDecodeBatch(req.Txs)
	}
	ctx.Logger().Info("1->sortTxs TxDecodeBatch time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(req.Txs), "height", ctx.BlockHeight())
	insertTime := time.Now()
	var (
		sortTxs                 = make([][]byte, 0)
		wg                      sync.WaitGroup
		nsMempool               = mempool.NewNamespaceMempool()
		beforeInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
		afterInterChainPairTxs  = make(map[string][]*cmtproto.NsInterChainTransactionIndex)

		listNamespaceByteTxs = func(byteTxs []sdk.ByteTx, nsTable []*cmtproto.NamespaceTableEntries) map[uint64][]sdk.ByteTx {
			nsByteTxs := make(map[uint64][]sdk.ByteTx)
			lastOffset := int32(0)
			for _, entry := range nsTable {
				namespaceTxs := byteTxs[lastOffset:entry.Offset]
				namespaceId := entry.Namespace
				lastOffset = entry.Offset
				nsByteTxs[namespaceId] = namespaceTxs
			}
			return nsByteTxs
		}

		nsMempoolInsertTxs = func(ctx sdk.Context, namespaceTxs []sdk.ByteTx, namespaceId uint64) {
			defer wg.Done()
			singleStartTime := time.Now()
			for idx, byteTx := range namespaceTxs {
				tx := byteTx.Tx
				txBz := byteTx.TxBytes
				if tx == nil || txBz == nil {
					continue
				}

				txIdBz, isCross, _ := app.crossNamespaceTransactionIdentifierHandler(ctx.WithTxBytes(txBz), tx)
				if isCross {
					txId := tmbytes.HexBytes(txIdBz).String()
					if _, ok := beforeInterChainPairTxs[txId]; ok {
						beforeInterChainPairTxs[txId] = append(beforeInterChainPairTxs[txId], &cmtproto.NsInterChainTransactionIndex{
							Namespace: namespaceId,
							Index:     int32(idx),
						})
					} else {
						beforeInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
							{
								Namespace: namespaceId,
								Index:     int32(idx),
							},
						}
					}
				}

				ctx = ctx.WithNamepace(namespaceId)
				_ = nsMempool.Insert(ctx, tx)

			}
			ctx.Logger().Info("2.1->sortTxs single namespace insert time", "used", fmt.Sprintf("%v ms", time.Since(singleStartTime).Milliseconds()), "size", len(namespaceTxs), "namespaceId", namespaceId, "height", ctx.BlockHeight())
		}
	)
	nsByteTxs := listNamespaceByteTxs(reqByteTxs, req.NsTable)

	for namespaceId, byteTxs := range nsByteTxs {
		wg.Add(1)
		go nsMempoolInsertTxs(ctx, byteTxs, namespaceId)
	}
	wg.Wait()
	ctx.Logger().Info("2->sortTxs total insert time", "used", fmt.Sprintf("%v ms", time.Since(insertTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "height", ctx.BlockHeight())

	sortTime := time.Now()
	nsIterators := nsMempool.SelectWithNsTable(ctx, req.Txs, req.NsTable)

	ctx.Logger().Info("3->sortTxs sort time", "used", fmt.Sprintf("%v ms", time.Since(sortTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "height", ctx.BlockHeight())

	var (
		offset  = 0
		nsTable = make([]*cmtproto.NamespaceTableEntries, 0)
		orders  = &cmtproto.Orders{
			NsTransactionOrders: &cmtproto.NsTransactionOrders{
				Entries: make([]*cmtproto.NsTransactionOrderEntry, 0)},
			NsInterChainTransactionOrders: &cmtproto.NsInterChainTransactionOrders{
				Entries: make([]*cmtproto.NsInterChainTransactionOrderEntry, 0),
			},
		}
		txIndex = 0
	)
	staticTime := time.Now()
	handleNsInterChainTransactionOrders := func(byteTx sdk.ByteTx, txIdx int, namespaceId uint64) bool {
		tx := byteTx.Tx
		txBz := byteTx.TxBytes
		txIdBz, isCross, isFirst := app.crossNamespaceTransactionIdentifierHandler(ctx.WithTxBytes(txBz), tx)
		if isCross {
			txId := tmbytes.HexBytes(txIdBz).String()
			if pairTxs, inPair := beforeInterChainPairTxs[txId]; inPair {
				if len(pairTxs) != 2 {
					return false
				}
			}

			if interTxs, ok := afterInterChainPairTxs[txId]; ok {
				interTxs = append(interTxs, &cmtproto.NsInterChainTransactionIndex{
					Namespace: namespaceId,
					Index:     int32(txIdx),
				})

				idxes := make([]*cmtproto.NsInterChainTransactionIndex, 2)

				if isFirst {
					idxes[0] = interTxs[1]
					idxes[1] = interTxs[0]
				} else {
					idxes[0] = interTxs[0]
					idxes[1] = interTxs[1]
				}

				entry := &cmtproto.NsInterChainTransactionOrderEntry{
					Indexes: idxes,
				}
				orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
			} else {
				afterInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
					{
						Namespace: namespaceId,
						Index:     int32(txIdx),
					},
				}
			}
		}
		return true
	}

	for _, nsIterator := range nsIterators {
		iterator := nsIterator.Iterator
		namespaceId := nsIterator.NamespaceId

		txSize := nsMempool.CountTxByNamespaceId(namespaceId)
		// Static analysis
		var (
			txs                  = make([]sdk.Tx, 0, txSize)
			byteTxs              = make([]sdk.ByteTx, 0, txSize)
			addressIndexMap      = make(map[string]int)
			lastTxIndexMap       = make(map[string]int)
			nsTransactionIndexes = make([]*cmtproto.NsTransactionIndexes, 0, txSize)
		)

		// If the iterator is nil use the NoOpMempool, get all namespace txs
		if iterator == nil {
			byteTxs = nsByteTxs[namespaceId]
		} else {
			for iterator != nil {
				tx := iterator.Tx()
				txs = append(txs, tx)
				iterator = iterator.Next()
			}
			batchTime := time.Now()
			byteTxs, _ = app.TxEncodeBatch(txs)
			ctx.Logger().Info("4.1->sortTxs iterator TxEncodeBatch  time", "used", fmt.Sprintf("%v ms", time.Since(batchTime).Milliseconds()), "namespaceId", namespaceId, "height", ctx.BlockHeight())
		}

		handleNsTransactionDependOrders := func(txIdx int, namespaceId uint64, from string, to string) {
			if lastToIdx, toExists := lastTxIndexMap[to]; toExists {
				if lastFromIdx, fromExists := lastTxIndexMap[from]; !fromExists || lastFromIdx != lastToIdx {
					entry := &cmtproto.NsInterChainTransactionOrderEntry{
						Indexes: []*cmtproto.NsInterChainTransactionIndex{
							{
								Namespace: namespaceId,
								Index:     int32(lastToIdx),
							},
							{
								Namespace: namespaceId,
								Index:     int32(txIdx),
							},
						},
					}
					orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
				}
			}
			lastTxIndexMap[from] = txIdx
			lastTxIndexMap[to] = txIdx
		}

		handleNsTransactionOrders := func(tx sdk.Tx, txIdx int, namespaceId uint64) {

			var (
				fromIdx    int
				toIdx      int
				fromExists bool
				toExists   bool
				from       string
				to         string
				curIdx     int
				isToSerial bool
				toIsSerial = func(namespaceId uint64, to string) bool {
					isContract, isContractSerial := app.contractStaticAnalysisResultHandler(ctx.WithNamepace(namespaceId), to)
					if isContract {
						return isContractSerial
					}
					return true
				}
			)

			defer func() {
				if isToSerial {
					handleNsTransactionDependOrders(txIdx, namespaceId, from, to)
				}
			}()

			from, to = app.txAccountInfoHandler(tx)

			fromIdx, fromExists = addressIndexMap[from]
			toIdx, toExists = addressIndexMap[to]

			if fromExists && toExists {
				//Both exist,add from index
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))

				//Check if they should be depends
				if toIsSerial(namespaceId, to) {
					isToSerial = true
				}
			} else if fromExists {
				// Only from exists, add to index
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
				addressIndexMap[to] = curIdx
			} else if toExists {
				// Only to exists, add from index
				if toIsSerial(namespaceId, to) {
					isToSerial = true
					//append to index
					curIdx = toIdx
					nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
					addressIndexMap[from] = curIdx
				} else {
					//create new from index
					nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
					curIdx = len(nsTransactionIndexes) - 1
					addressIndexMap[from] = curIdx
				}
			} else {
				// Neither exists, create both indices
				//create new from & to index
				nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
				curIdx = len(nsTransactionIndexes) - 1
				addressIndexMap[from] = curIdx
				addressIndexMap[to] = curIdx
			}
		}

		orderTime := time.Now()
		for _, byteTx := range byteTxs {
			txBz := byteTx.TxBytes
			tx := byteTx.Tx
			if tx == nil || txBz == nil {
				continue
			}
			ok := handleNsInterChainTransactionOrders(byteTx, txIndex, namespaceId)
			if !ok {
				continue
			}

			sortTxs = append(sortTxs, txBz)
			offset++

			handleNsTransactionOrders(tx, txIndex, namespaceId)
			txIndex++
		}
		ctx.Logger().Info("4.2->sortTxs orders time", "used", fmt.Sprintf("%v ms", time.Since(orderTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())

		if len(nsTransactionIndexes) > 0 {
			nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
				Namespace: namespaceId,
				Offset:    int32(offset),
			})
			orders.NsTransactionOrders.Entries = append(orders.NsTransactionOrders.Entries, &cmtproto.NsTransactionOrderEntry{
				Namespace: namespaceId,
				Indexes:   nsTransactionIndexes,
			})
		}

	}
	ctx.Logger().Info("4->sortTxs total orders time", "used", fmt.Sprintf("%v ms", time.Since(staticTime).Milliseconds()), "size", len(sortTxs), "nsTable", nsTable, "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())
	ctx.Logger().Info("5->sortTxs total time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(sortTxs), "nsTable", nsTable, "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "reqSortType", req.SortType, "height", ctx.BlockHeight())

	return &abci.ResponseSortTxs{Txs: sortTxs, NsTable: nsTable, Orders: orders}, nil
}
