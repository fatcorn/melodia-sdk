package types

import (
	"cosmossdk.io/core/appmodule"
	abci "github.com/cometbft/cometbft/abci/types"
	tdmtypes "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	"math"
)

// InitChainer initializes application state at genesis
type InitChainer func(ctx Context, req *abci.RequestInitChain) (*abci.ResponseInitChain, error)

// PrepareCheckStater runs code during commit after the block has been committed, and the `checkState`
// has been branched for the new block.
type PrepareCheckStater func(ctx Context)

// Precommiter runs code during commit immediately before the `deliverState` is written to the `rootMultiStore`.
type Precommiter func(ctx Context)

// PeerFilter responds to p2p filtering queries from Tendermint
type PeerFilter func(info string) *abci.ResponseQuery

// ProcessProposalHandler defines a function type alias for processing a proposer
type ProcessProposalHandler func(Context, *abci.RequestProcessProposal) (*abci.ResponseProcessProposal, error)

// PrepareProposalHandler defines a function type alias for preparing a proposal
type PrepareProposalHandler func(Context, *abci.RequestPrepareProposal) (*abci.ResponsePrepareProposal, error)

// ExtendVoteHandler defines a function type alias for extending a pre-commit vote.
type ExtendVoteHandler func(Context, *abci.RequestExtendVote) (*abci.ResponseExtendVote, error)

// VerifyVoteExtensionHandler defines a function type alias for verifying a
// pre-commit vote extension.
type VerifyVoteExtensionHandler func(Context, *abci.RequestVerifyVoteExtension) (*abci.ResponseVerifyVoteExtension, error)

// PreBlocker runs code before the `BeginBlocker` and defines a function type alias for executing logic right
// before FinalizeBlock is called (but after its context has been set up). It is
// intended to allow applications to perform computation on vote extensions and
// persist their results in state.
//
// Note: returning an error will make FinalizeBlock fail.
type PreBlocker func(Context, *abci.RequestFinalizeBlock) (*ResponsePreBlock, error)

// BeginBlocker defines a function type alias for executing application
// business logic before transactions are executed.
//
// Note: The BeginBlock ABCI method no longer exists in the ABCI specification
// as of CometBFT v0.38.0. This function type alias is provided for backwards
// compatibility with applications that still use the BeginBlock ABCI method
// and allows for existing BeginBlock functionality within applications.
type BeginBlocker func(Context) (BeginBlock, error)

// EndBlocker defines a function type alias for executing application
// business logic after transactions are executed but before committing.
//
// Note: The EndBlock ABCI method no longer exists in the ABCI specification
// as of CometBFT v0.38.0. This function type alias is provided for backwards
// compatibility with applications that still use the EndBlock ABCI method
// and allows for existing EndBlock functionality within applications.
type EndBlocker func(Context) (EndBlock, error)

// NamespaceMakeBlock
type NamespaceMakeBlock func(Context, *[]abci.Event) (map[string]appmodule.NamespaceBlockData, error)

// NamespaceBlockPreparer
type NamespaceBlockPreparer func(Context, [][]byte, []*tdmtypes.NamespaceTableEntries) (map[string]appmodule.NamespaceBlockData, error)

// InternalEventDetector
type InternalEventDetector func(Context, []Tx) (InternalEvents, error)

// EndBlock defines a type which contains endblock events and validator set updates
type EndBlock struct {
	ValidatorUpdates []abci.ValidatorUpdate
	Events           []abci.Event
}

// BeginBlock defines a type which contains beginBlock events
type BeginBlock struct {
	Events []abci.Event
}

// InternalEvent defines a type which contains internal events from InternalEvent detector
type InternalEvents struct {
	Events []abci.Event
}

type ResponsePreBlock struct {
	ConsensusParamsChanged bool
}

func (r ResponsePreBlock) IsConsensusParamsChanged() bool {
	return r.ConsensusParamsChanged
}

type ByteTx struct {
	Tx
	TxBytes   []byte
	GasWanted int64
}

type NamespaceByteTxs struct {
	NamespaceId uint64
	Txs         []*ByteTx
}

func ListNamespaceByteTxs(byteTxs []*ByteTx, nsTable []*tdmtypes.NamespaceTableEntries) map[uint64][]*ByteTx {
	nsByteTxs := make(map[uint64][]*ByteTx)
	lastOffset := int32(0)
	for _, entry := range nsTable {
		namespaceTxs := byteTxs[lastOffset:entry.Offset]
		namespaceId := entry.Namespace
		lastOffset = entry.Offset
		nsByteTxs[namespaceId] = namespaceTxs
	}
	return nsByteTxs
}

func ListNamespaceByteTxsAndTotalBytes(txs []*ByteTx, nsTable []*tdmtypes.NamespaceTableEntries) ([]NamespaceByteTxs, int64, int64) {
	nsTxs := make([]NamespaceByteTxs, 0)
	lastOffset := 0
	totalBytes := int64(0)
	totalGas := int64(0)
	for _, entry := range nsTable {
		namespaceTxs := txs[lastOffset:entry.Offset]
		memTotalBytes, memTotalGas := TotalBytesAndGas(namespaceTxs)
		nsTxs = append(nsTxs, NamespaceByteTxs{
			NamespaceId: entry.Namespace,
			Txs:         namespaceTxs,
		})
		lastOffset = int(entry.Offset)
		totalBytes += memTotalBytes
		totalGas += memTotalGas
	}
	return nsTxs, totalBytes, totalGas
}

func TotalBytesAndGas(txs []*ByteTx) (int64, int64) {
	totalBytes := int64(0)
	totalGas := int64(0)

	for _, tx := range txs {
		if tx.Tx != nil {
			if gasTx, ok := tx.Tx.(FeeTx); ok {
				tx.GasWanted = int64(gasTx.GetGas())
				totalBytes += int64(len(tx.TxBytes))
				totalGas += tx.GasWanted
			}
		}
	}
	return totalBytes, totalGas
}

// ReapMaxBytesMaxGasNsTable safely handles concurrent use by multiple goroutines.
func ReapMaxBytesMaxGasNsTable(maxBytes, maxGas int64, txs []*ByteTx, nsTable []*tdmtypes.NamespaceTableEntries) ([]*ByteTx, []*tdmtypes.NamespaceTableEntries) {
	var (
		overflowRatio float64
		totalBytes    int64
		totalGas      int64
		nsMemTxs      []NamespaceByteTxs
	)

	// List the transactions in namespaces and their total sizes.
	nsMemTxs, totalBytes, totalGas = ListNamespaceByteTxsAndTotalBytes(txs, nsTable)

	// Early exit if no overflow conditions are met.
	if (maxBytes == -1 || totalBytes <= maxBytes) && (maxGas == -1 || totalGas <= maxGas) {
		return txs, nsTable
	}

	// Initialize ratios and compute the overflow ratios only if applicable.
	overflowBytesRatio := 0.0
	overflowGasRatio := 0.0

	if maxBytes > -1 && totalBytes > maxBytes {
		overflowBytesRatio = float64(maxBytes) / float64(totalBytes)
	}

	if maxGas > -1 && totalGas > maxGas {
		overflowGasRatio = float64(maxGas) / float64(totalGas)
	}

	// Choose the larger overflow ratio.
	overflowRatio = math.Max(overflowBytesRatio, overflowGasRatio)

	// Prepare the new list of transactions with the adjusted limits.
	nsTxs := make([]NamespaceByteTxs, 0, len(nsMemTxs))

	for _, nsMemTx := range nsMemTxs {
		memTxs := nsMemTx.Txs
		namespaceId := nsMemTx.NamespaceId

		// Apply the limit based on the overflow ratio.
		limit := len(memTxs)
		if overflowRatio > 0 {
			limit = int(math.Ceil(overflowRatio * float64(len(memTxs))))
		}

		// Limit the number of transactions in the namespace if necessary.
		if limit < len(memTxs) {
			memTxs = memTxs[:limit]
		}

		// Append the adjusted namespace transactions.
		nsTxs = append(nsTxs, NamespaceByteTxs{
			NamespaceId: namespaceId,
			Txs:         memTxs,
		})
	}

	// Reap the transactions and return the result.
	return ReapNsTxs(nsTxs, maxBytes, maxGas)
}

// ReapNsTxs reap namespace txs result
func ReapNsTxs(nsTxs []NamespaceByteTxs, maxBytes, maxGas int64) ([]*ByteTx, []*tdmtypes.NamespaceTableEntries) {
	var (
		totalGas    int64
		runningSize int64
		nsTable     []*tdmtypes.NamespaceTableEntries
	)
	// TODO: we will get a performance boost if we have a good estimate of avg
	// size per tx, and set the initial capacity based off of that.
	// txs := make([]types.Tx, 0, cmtmath.MinInt(mem.txs.Len(), max/mem.avgTxSize))
	txs := make([]*ByteTx, 0)

	offset := 0
	for _, nsTx := range nsTxs {
		byteTxs := nsTx.Txs
		for _, byteTx := range byteTxs {
			dataSize := types.ComputeProtoSizeForTxs([]types.Tx{byteTx.TxBytes})
			// Check total size requirement
			if maxBytes > -1 && runningSize+dataSize > maxBytes {
				break
			}
			runningSize += dataSize

			// Check total gas requirement.
			// If maxGas is negative, skip this check.
			// Since newTotalGas < masGas, which
			// must be non-negative, it follows that this won't overflow.
			newTotalGas := totalGas + byteTx.GasWanted
			if maxGas > -1 && newTotalGas > maxGas {
				break
			}

			txs = append(txs, byteTx)
			totalGas = newTotalGas
			offset++
		}
		nsTable = append(nsTable, &tdmtypes.NamespaceTableEntries{
			Namespace: nsTx.NamespaceId,
			Offset:    int32(offset),
		})
	}
	return txs, nsTable
}
