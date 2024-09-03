package types

import (
	"cosmossdk.io/errors"
)

// IBC transfer sentinel errors
var (
	ErrInvalidPacketTimeout    = errors.Register(ModuleName, 2, "invalid packet timeout")
	ErrInvalidDenomForTransfer = errors.Register(ModuleName, 3, "invalid denomination for cross-chain transfer")
	ErrInvalidVersion          = errors.Register(ModuleName, 4, "invalid ICS20 version")
	ErrInvalidAmount           = errors.Register(ModuleName, 5, "invalid token amount")
	ErrTraceNotFound           = errors.Register(ModuleName, 6, "denomination trace not found")
	ErrSendDisabled            = errors.Register(ModuleName, 7, "fungible token transfers from this chain are disabled")
	ErrReceiveDisabled         = errors.Register(ModuleName, 8, "fungible token transfers to this chain are disabled")
	ErrMaxTransferChannels     = errors.Register(ModuleName, 9, "max transfer channels")
	ErrInvalidAuthorization    = errors.Register(ModuleName, 10, "invalid transfer authorization")
	ErrInvalidMemo             = errors.Register(ModuleName, 11, "invalid memo")
	// ErrUnauthorized is used whenever a request without sufficient
	// authorization is handled.
	ErrUnauthorized = errors.Register(ModuleName, 12, "unauthorized")
)
