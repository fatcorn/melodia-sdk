package keeper

import (
	"context"
	"cosmossdk.io/collections"
	scheduler "cosmossdk.io/store/types/occ"
	"errors"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// NewAccountWithAddress implements AccountKeeperI.
func (ak AccountKeeper) NewAccountWithAddress(ctx context.Context, addr sdk.AccAddress) sdk.AccountI {
	acc := ak.proto()
	err := acc.SetAddress(addr)
	if err != nil {
		panic(err)
	}

	return ak.NewAccount(ctx, acc)
}

// NewAccount sets the next account number to a given account interface
func (ak AccountKeeper) NewAccount(ctx context.Context, acc sdk.AccountI) sdk.AccountI {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	accountNumber := sdkCtx.NextAccountNumber(acc.GetAddress().String())
	if accountNumber == nil {
		nextAccountNumber := ak.NextAccountNumber(ctx)
		accountNumber = &nextAccountNumber
		if sdkCtx.ExecMode() == sdk.ExecModeFinalize {
			sdkCtx.Logger().Warn("new account without context account number, new next account number", "accountNumber", nextAccountNumber, "execMode=", sdkCtx.ExecMode(), "address", acc.GetAddress().String(), "namespace", sdkCtx.Namespace(), "height", sdkCtx.BlockHeight())
		}
	}
	if err := acc.SetAccountNumber(*accountNumber); err != nil {
		panic(err)
	}
	if sdkCtx.ExecMode() == sdk.ExecModeFinalize {
		sdkCtx.Logger().Info("new account", "accountNumber", acc.GetAccountNumber(), "execMode=", sdkCtx.ExecMode(), "address", acc.GetAddress().String(), "namespace", sdkCtx.Namespace(), "height", sdkCtx.BlockHeight())
	}
	return acc
}

// HasAccount implements AccountKeeperI.
func (ak AccountKeeper) HasAccount(ctx context.Context, addr sdk.AccAddress) bool {
	has, _ := ak.Accounts.Has(ctx, addr)
	return has
}

// GetAccount implements AccountKeeperI.
func (ak AccountKeeper) GetAccount(ctx context.Context, addr sdk.AccAddress) sdk.AccountI {
	acc, err := ak.Accounts.Get(ctx, addr)
	if err != nil && !errors.Is(err, collections.ErrNotFound) {
		panic(err)
	}
	return acc
}

// GetAllAccounts returns all accounts in the accountKeeper.
func (ak AccountKeeper) GetAllAccounts(ctx context.Context) (accounts []sdk.AccountI) {
	ak.IterateAccounts(ctx, func(acc sdk.AccountI) (stop bool) {
		accounts = append(accounts, acc)
		return false
	})

	return accounts
}

// SetAccount implements AccountKeeperI.
func (ak AccountKeeper) SetAccount(ctx context.Context, acc sdk.AccountI) {
	err := ak.Accounts.Set(ctx, acc.GetAddress(), acc)
	if err != nil {
		if errors.Is(err, collections.ErrConflict) {
			sdkCtx := sdk.UnwrapSDKContext(ctx)
			if sdkCtx.ExecMode() == sdk.ExecModeFinalize {
				if !sdkCtx.Synchronous() {
					sdkCtx.Logger().Error("Set account panic with next account number", "accountNumber", acc.GetAccountNumber(), "address", acc.GetAddress().String(), "height", sdkCtx.BlockHeight())
					panic(scheduler.NewErrNextAccountNumber(sdkCtx.TxIndex()))
				}
			}

		}
		panic(err)
	}
}

// RemoveAccount removes an account for the account mapper store.
// NOTE: this will cause supply invariant violation if called
func (ak AccountKeeper) RemoveAccount(ctx context.Context, acc sdk.AccountI) {
	err := ak.Accounts.Remove(ctx, acc.GetAddress())
	if err != nil {
		panic(err)
	}
}

// IterateAccounts iterates over all the stored accounts and performs a callback function.
// Stops iteration when callback returns true.
func (ak AccountKeeper) IterateAccounts(ctx context.Context, cb func(account sdk.AccountI) (stop bool)) {
	err := ak.Accounts.Walk(ctx, nil, func(_ sdk.AccAddress, value sdk.AccountI) (bool, error) {
		return cb(value), nil
	})
	if err != nil {
		panic(err)
	}
}
