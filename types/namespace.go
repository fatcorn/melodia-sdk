package types

import (
	"encoding/json"
	"github.com/cometbft/cometbft/types"
	"math/big"
)

// NamespacePortWaitCh Define the namespace port for json-rpc
var NamespacePortWaitCh = make(chan types.NamespacePort)

// NamespaceGenesis the module genesis
type NamespaceGenesis struct {
	Data        json.RawMessage // Original data
	NamespaceId types.Namespace // Additional namespaceId field
	ChainID     *big.Int
	ModuleName  string
	TmWsPort    int
}
