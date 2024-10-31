package keeper

import (
	"context"
	"cosmossdk.io/core/store"
	"cosmossdk.io/log"
	"fmt"
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/types"
)

type Keeper struct {
	storeService store.KVStoreService
	cdc          codec.BinaryCodec
	logger       log.Logger
	Router       *types.Router
}

func ContextWithNamespace(ctx context.Context, namespace uint64) sdk.Context {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	return sdkCtx.WithNamepace(namespace)
}

func NewKeeper(
	storeService store.KVStoreService,
	logger log.Logger,
	cdc codec.BinaryCodec,
) Keeper {
	return Keeper{
		cdc:          cdc,
		storeService: storeService,
		logger:       logger,
	}
}

// SetRouter sets the Router in IBC Keeper and seals it. The method panics if
// there is an existing router that's already sealed.
func (k *Keeper) SetRouter(rtr *types.Router) {
	if k.Router != nil && k.Router.Sealed() {
		panic("cannot reset a sealed router")
	}

	k.Router = rtr
	k.Router = rtr
	k.Router.Seal()
}

// GetSourceTransactionModule get GetSourceTransactionModule
func (k *Keeper) GetSourceTransactionModule(ctx sdk.Context, sourceTxId []byte) (string, error) {
	kvStore := k.storeService.OpenKVStore(ctx)
	val, err := kvStore.Get(types.GetSourceTransactionModuleKey(sourceTxId))
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// SetSourceTransactionModule set SetSourceTransactionModule
func (k *Keeper) SetSourceTransactionModule(ctx sdk.Context, sourceTxId []byte, module string) error {
	kvStore := k.storeService.OpenKVStore(ctx)

	key := types.GetSourceTransactionModuleKey(sourceTxId)
	_, err := kvStore.Get(key)
	if err != nil {
		return fmt.Errorf("sourceTxId already exist")
	}

	return kvStore.Set(key, []byte(module))
}

// GetTransactionSourcePacket get GetTransactionSourcePacket
func (k *Keeper) GetTransactionSourcePacket(ctx sdk.Context, sourceTxId []byte) (*types.BasePacket, error) {
	kvStore := k.storeService.OpenKVStore(ctx)
	val, err := kvStore.Get(types.GetTransactionSourcePacketKey(sourceTxId))
	if err != nil {
		return nil, err
	}

	var ret types.BasePacket
	k.cdc.MustUnmarshal(val, &ret)

	return &ret, nil
}

// SetTransactionSourcePacket set SetTransactionSourcePacket
func (k *Keeper) SetTransactionSourcePacket(ctx sdk.Context, sourceTxId []byte, packet *types.BasePacket) error {
	kvStore := k.storeService.OpenKVStore(ctx)

	key := types.GetTransactionSourcePacketKey(sourceTxId)
	_, err := kvStore.Get(key)
	if err != nil {
		return fmt.Errorf("sourceTxId already exist")
	}

	val := k.cdc.MustMarshal(packet)

	return kvStore.Set(key, val)
}

// DeleteTransactionSourcePacket del DeleteTransactionSourcePacket
func (k *Keeper) DeleteTransactionSourcePacket(ctx sdk.Context, sourceTxId []byte) error {
	kvStore := k.storeService.OpenKVStore(ctx)

	key := types.GetTransactionSourcePacketKey(sourceTxId)
	return kvStore.Delete(key)
}
