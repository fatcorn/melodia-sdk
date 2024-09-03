package types

import (
	"context"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/types"
)

// AccountKeeper defines the contract required for account APIs.
type AccountKeeper interface {
	GetModuleAddress(name string) sdk.AccAddress
	//GetModuleAccount(ctx sdk.Context, name string) authtypes.ModuleAccountI
}

// BankKeeper defines the expected bank keeper
type BankKeeper interface {
	authtypes.BankKeeper
	MintCoins(ctx context.Context, moduleName string, amt sdk.Coins) error
	BurnCoins(ctx context.Context, moduleName string, amt sdk.Coins) error
	SendCoinsFromModuleToAccount(ctx context.Context, senderModule string, recipientAddr sdk.AccAddress, amt sdk.Coins) error
	SendCoinsFromAccountToModule(ctx context.Context, senderAddr sdk.AccAddress, recipientModule string, amt sdk.Coins) error
	BlockedAddr(addr sdk.AccAddress) bool
	IsSendEnabledCoin(ctx context.Context, coin sdk.Coin) bool
	GetBalance(ctx context.Context, addr sdk.AccAddress, denom string) sdk.Coin
	GetAllBalances(ctx context.Context, addr sdk.AccAddress) sdk.Coins
}

type PIBCKeeper interface {
	SetSourceTransactionModule(ctx sdk.Context, sourceTxId []byte, module string) error
	GetTransactionSourcePacket(ctx sdk.Context, sourceTxId []byte) (*types.BasePacket, error)
	SetTransactionSourcePacket(ctx sdk.Context, sourceTxId []byte, packet *types.BasePacket) error
	DeleteTransactionSourcePacket(ctx sdk.Context, sourceTxId []byte) error
}
