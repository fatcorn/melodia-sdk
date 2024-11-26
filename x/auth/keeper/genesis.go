package keeper

import (
	"fmt"
	tmtypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/auth/types"
)

// InitGenesis - Init store state from genesis data
//
// CONTRACT: old coins from the FeeCollectionKeeper need to be transferred through
// a genesis port script to the new fee collector account
func (ak AccountKeeper) InitGenesis(ctx sdk.Context, data types.GenesisState) {
	if err := ak.Params.Set(ctx, data.Params); err != nil {
		panic(err)
	}

	accounts, err := types.UnpackAccounts(data.Accounts)
	if err != nil {
		panic(err)
	}
	accounts = types.SanitizeGenesisAccounts(accounts)

	// Set the accounts and make sure the global account number matches the largest account number (even if zero).
	var lastAccNum *uint64
	for _, acc := range accounts {
		accNum := acc.GetAccountNumber()
		for lastAccNum == nil || *lastAccNum < accNum {
			n := ctx.NextAccountNumber(acc.GetAddress().String())
			if n == nil {
				n2 := ak.NextAccountNumber(ctx)
				n = &n2
			}
			lastAccNum = n
		}
		ak.SetAccount(ctx, acc)
	}

	ak.GetModuleAccount(ctx, types.FeeCollectorName)
}

// ExportGenesis returns a GenesisState for a given context and keeper
func (ak AccountKeeper) ExportGenesis(ctx sdk.Context) *types.GenesisState {
	params := ak.GetParams(ctx)

	var genAccounts types.GenesisAccounts
	ak.IterateAccounts(ctx, func(account sdk.AccountI) bool {
		genAccount := account.(types.GenesisAccount)
		genAccounts = append(genAccounts, genAccount)
		return false
	})

	return types.NewGenesisState(params, genAccounts)
}

// ChainserviceHandler the namespace init genesis handler
type ChainserviceHandler struct {
	keeper AccountKeeper
}

// NamespaceInitGenesis namespace initializes the module's state from a provided genesis state.
func (c ChainserviceHandler) NamespaceInitGenesis(ctx sdk.Context, genesis sdk.NamespaceGenesis) error {
	ctx.Logger().Info(fmt.Sprintf("Namespace init genesis %s namespaceId= %v", types.ModuleName, genesis.NamespaceId))
	cdc := c.keeper.GetCdc().(codec.Codec)

	var genesisState types.GenesisState
	err := cdc.UnmarshalJSON(genesis.Data, &genesisState)
	if err != nil {
		return err
	}

	c.keeper.InitGenesis(ctx, genesisState)
	return nil
}

func (_c ChainserviceHandler) ListDependentModulesStoreNames() []tmtypes.DependModuleStoreInfo {
	return []tmtypes.DependModuleStoreInfo{}
}

// NewChainserviceHandler create new handler
func NewChainserviceHandler(k AccountKeeper) ChainserviceHandler {
	return ChainserviceHandler{keeper: k}
}
