package types

import "cosmossdk.io/errors"

// x/bank module sentinel errors
var (
	ErrSourceTxIdNoteFind = errors.Register(ModuleName, 2, "invalid RecvPacket, not find source tx id")
	ErrInvalidPIBCModule  = errors.Register(ModuleName, 3, "invalid PIBC module, module not registered in pibc module")
)
