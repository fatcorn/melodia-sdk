package types

import (
	"cosmossdk.io/core/appmodule"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/crypto/tmhash"
	tmbytes "github.com/cometbft/cometbft/libs/bytes"
	tdmtypes "github.com/cometbft/cometbft/proto/tendermint/types"
	tmtypes "github.com/cometbft/cometbft/types"
	"math/big"
	"strconv"
	"strings"
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

type SortTxHandler struct {
	CrossNamespaceTransactionIdentifierHandler func(ctx Context, tx Tx) ([]byte, bool, bool)
	ContractStaticAnalysisResultHandler        func(ctx Context, contractAddress string) (bool, bool)
	CheckTxFeeHandler                          func(ctx Context, baseFee *big.Int, tx Tx) bool
	BaseFeeHandler                             func(ctx Context) *big.Int
	GetAccountNonceHandler                     func(ctx Context, address string) (uint64, error)
	IsGapped                                   func(ctx Context, tx MempoolTx) (bool, bool)
}

type MempoolTx struct {
	Tx
	SortTxHandler
	TxBytes  []byte
	TxGas    uint64
	GasPrice *big.Int
	From     string
	To       string
	Nonce    uint64
	IsNoOp   bool
}

// NamespaceSelectedTxs is used for sorting transactions within a namespace.
type NamespaceSelectedTxs struct {
	Txs     []MempoolTx                       // List of transactions
	NsTable []*tdmtypes.NamespaceTableEntries // Table entries for namespaces

	ReplacedTxs     []MempoolTx
	ReplacedNsTable []*tdmtypes.NamespaceTableEntries
}

type NamespaceMempoolTxs struct {
	NamespaceId uint64
	Txs         []MempoolTx
}

func (tx *MempoolTx) NonceHash(namespaceId uint64) string {
	var builder strings.Builder
	builder.WriteString(strconv.FormatUint(namespaceId, 10))
	builder.WriteString(tx.From)
	builder.WriteString(strconv.FormatUint(tx.Nonce, 10))
	return tmbytes.HexBytes(tmhash.Sum([]byte(builder.String()))).String()
}

func (tx *MempoolTx) Hash() string {
	return tmbytes.HexBytes(tmtypes.Tx(tx.TxBytes).Hash()).String()
}

// GasFeeCapCmp compares the fee cap of two transactions.
func (tx *MempoolTx) GasFeeCapCmp(other *MempoolTx) int {
	return tx.GasPrice.Cmp(other.GasPrice)
}

// GasFeeCapCmp compares the fee cap of two transactions.
func (tx *MempoolTx) GasFeeCap() *big.Int {
	var gasFeeCap int64
	feeTx, ok := tx.Tx.(FeeTx)
	if ok {
		fee := feeTx.GetFee()
		gas := feeTx.GetGas()
		if gas <= 0 {
			return big.NewInt(0)
		}
		for _, c := range fee {
			gasPrice := c.Amount.QuoRaw(int64(gas))
			if gasPrice.IsInt64() {
				gasFeeCap = gasPrice.Int64()
			}
			if gasFeeCap > 0 {
				return big.NewInt(gasFeeCap)
			}
		}
	}
	return big.NewInt(0)
}

func (tx *MempoolTx) Gas() uint64 {
	feeTx, ok := tx.Tx.(FeeTx)
	if ok {
		return feeTx.GetGas()
	}
	return 0
}

func ListNamespaceMemTxs(txs []MempoolTx, nsTable []*tdmtypes.NamespaceTableEntries) map[uint64][]MempoolTx {
	nsMempoolTxs := make(map[uint64][]MempoolTx)
	lastOffset := int32(0)
	for _, entry := range nsTable {
		namespaceTxs := txs[lastOffset:entry.Offset]
		namespaceId := entry.Namespace
		lastOffset = entry.Offset
		nsMempoolTxs[namespaceId] = namespaceTxs
	}
	return nsMempoolTxs
}
