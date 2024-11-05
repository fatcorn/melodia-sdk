package types

import (
	"cosmossdk.io/store/multiversion"
	sdk "cosmossdk.io/store/types"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/proto/tendermint/types"
)

// DeliverTxEntry represents an individual transaction's request within a batch.
// This can be extended to include tx-level metadata
type DeliverTxEntry struct {
	Request            []byte
	SdkTx              Tx
	Checksum           [32]byte
	AbsoluteIndex      int
	EstimatedWritesets MappedWritesets
	ToAddress          []string
	//TxTracer           TxTracer
}

// EstimatedWritesets represents an estimated writeset for a transaction mapped by storekey to the writeset estimate.
type MappedWritesets map[sdk.StoreKey]multiversion.WriteSet

// DeliverTxBatchRequest represents a request object for a batch of transactions.
// This can be extended to include request-level tracing or metadata
type DeliverTxBatchRequest struct {
	TxEntries []*DeliverTxEntry
	Orders    *types.Orders
}

// DeliverTxResult represents an individual transaction's response within a batch.
// This can be extended to include tx-level tracing or metadata
type DeliverTxResult struct {
	Response abci.ExecTxResult
}

// DeliverTxBatchResponse represents a response object for a batch of transactions.
// This can be extended to include response-level tracing or metadata
type DeliverTxBatchResponse struct {
	Results      []*DeliverTxResult
	NamespaceIds []uint64
}
