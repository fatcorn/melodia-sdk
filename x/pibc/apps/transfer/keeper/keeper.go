package keeper

import (
	"bytes"
	"context"
	sdkerrors "cosmossdk.io/errors"
	sdkmath "cosmossdk.io/math"
	storetypes "cosmossdk.io/store/types"
	"fmt"
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	paramtypes "github.com/cosmos/cosmos-sdk/x/params/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/apps/transfer/types"
	pibctypes "github.com/cosmos/cosmos-sdk/x/pibc/types"
	"strings"
)

// Keeper defines the IBC fungible transfer keeper
type Keeper struct {
	storeKey   storetypes.StoreKey
	cdc        codec.BinaryCodec
	paramSpace paramtypes.Subspace

	authKeeper types.AccountKeeper
	bankKeeper types.BankKeeper
	pibckeeper types.PIBCKeeper
}

// NewKeeper creates a new IBC transfer Keeper instance
func NewKeeper(
	cdc codec.BinaryCodec, key storetypes.StoreKey, paramSpace paramtypes.Subspace,
	authKeeper types.AccountKeeper, bankKeeper types.BankKeeper, pibcKeeper types.PIBCKeeper,
) Keeper {
	// ensure ibc transfer module account is set
	if addr := authKeeper.GetModuleAddress(types.ModuleName); addr == nil {
		panic("the IBC transfer module account has not been set")
	}

	return Keeper{
		cdc:        cdc,
		storeKey:   key,
		paramSpace: paramSpace,
		authKeeper: authKeeper,
		bankKeeper: bankKeeper,
		pibckeeper: pibcKeeper,
	}
}

func ContextWithNamespace(ctx context.Context, namespace uint64) sdk.Context {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	return sdkCtx.WithNamepace(namespace)
}

// SetTotalEscrowForDenom stores the total amount of source chain tokens that are in escrow.
// Amount is stored in state if and only if it is not equal to zero. The function will panic
// if the amount is negative.
func (k Keeper) SetTotalEscrowForDenom(ctx sdk.Context, coin sdk.Coin) {
	if coin.Amount.IsNegative() {
		panic(fmt.Sprintf("amount cannot be negative: %s", coin.Amount))
	}

	store := ctx.KVStore(k.storeKey)
	key := types.TotalEscrowForDenomKey(coin.Denom)

	if coin.Amount.IsZero() {
		store.Delete(key) // delete the key since Cosmos SDK x/bank module will prune any non-zero balances
		return
	}

	bz := k.cdc.MustMarshal(&sdk.IntProto{Int: coin.Amount})
	store.Set(key, bz)
}

// GetAllTotalEscrowed returns the escrow information for all the denominations.
func (k Keeper) GetAllTotalEscrowed(ctx sdk.Context) sdk.Coins {
	var escrows sdk.Coins
	k.IterateTokensInEscrow(ctx, []byte(types.KeyTotalEscrowPrefix), func(denomEscrow sdk.Coin) bool {
		escrows = escrows.Add(denomEscrow)
		return false
	})

	return escrows
}

// IterateTokensInEscrow iterates over the denomination escrows in the store
// and performs a callback function. Denominations for which an invalid value
// (i.e. not integer) is stored, will be skipped.
func (k Keeper) IterateTokensInEscrow(ctx sdk.Context, prefix []byte, cb func(denomEscrow sdk.Coin) bool) {
	store := ctx.KVStore(k.storeKey)
	iterator := store.Iterator(prefix, types.PrefixEndBytes(prefix))

	defer sdk.LogDeferred(ctx.Logger(), func() error { return iterator.Close() })
	for ; iterator.Valid(); iterator.Next() {
		denom := strings.TrimPrefix(string(iterator.Key()), fmt.Sprintf("%s/", types.KeyTotalEscrowPrefix))
		if strings.TrimSpace(denom) == "" {
			continue // denom is empty
		}

		amount := sdk.IntProto{}
		if err := k.cdc.Unmarshal(iterator.Value(), &amount); err != nil {
			continue // total escrow amount cannot be unmarshalled to integer
		}

		denomEscrow := sdk.NewCoin(denom, amount.Int)
		if cb(denomEscrow) {
			break
		}
	}
}

// GetTotalEscrowForDenom gets the total amount of source chain tokens that
// are in escrow, keyed by the denomination.
//
// NOTE: if there is no value stored in state for the provided denom then a new Coin is returned for the denom with an initial value of zero.
// This accommodates callers to simply call `Add()` on the returned Coin as an empty Coin literal (e.g. sdk.Coin{}) will trigger a panic due to the absence of a denom.
func (k Keeper) GetTotalEscrowForDenom(ctx sdk.Context, denom string) sdk.Coin {
	store := ctx.KVStore(k.storeKey)
	bz := store.Get(types.TotalEscrowForDenomKey(denom))
	if len(bz) == 0 {
		return sdk.NewCoin(denom, sdkmath.ZeroInt())
	}

	amount := sdk.IntProto{}
	k.cdc.MustUnmarshal(bz, &amount)

	return sdk.NewCoin(denom, amount.Int)
}

// escrowToken will send the given token from the provided sender to the escrow address. It will also
// update the total escrowed amount by adding the escrowed token to the current total escrow.
func (k Keeper) escrowToken(ctx sdk.Context, sender, escrowAddress sdk.AccAddress, token sdk.Coin) error {
	if err := k.bankKeeper.SendCoins(ctx, sender, escrowAddress, sdk.NewCoins(token)); err != nil {
		// failure is expected for insufficient balances
		return err
	}

	// track the total amount in escrow keyed by denomination to allow for efficient iteration
	ctx = ctx.WithNamepace(0)
	currentTotalEscrow := k.GetTotalEscrowForDenom(ctx, token.GetDenom())
	newTotalEscrow := currentTotalEscrow.Add(token)
	k.SetTotalEscrowForDenom(ctx, newTotalEscrow)

	return nil
}

// UnescrowToken will send the given token from the escrow address to the provided receiver. It will also
// update the total escrow by deducting the unescrowed token from the current total escrow.
func (k Keeper) UnescrowToken(ctx sdk.Context, escrowAddress, receiver sdk.AccAddress, token sdk.Coin) error {
	if err := k.bankKeeper.SendCoins(ctx, escrowAddress, receiver, sdk.NewCoins(token)); err != nil {
		// NOTE: this error is only expected to occur given an unexpected bug or a malicious
		// counterparty module. The bug may occur in bank or any part of the code that allows
		// the escrow address to be drained. A malicious counterparty module could drain the
		// escrow address by allowing more tokens to be sent back then were escrowed.
		return sdkerrors.Wrap(err, "unable to unescrow tokens, this may be caused by a malicious counterparty module or a bug: please open an issue on counterparty module")
	}

	// track the total amount in escrow keyed by denomination to allow for efficient iteration
	ctx = ctx.WithNamepace(0)
	currentTotalEscrow := k.GetTotalEscrowForDenom(ctx, token.GetDenom())
	newTotalEscrow := currentTotalEscrow.Sub(token)
	k.SetTotalEscrowForDenom(ctx, newTotalEscrow)

	return nil
}

// GetTransferDirection g
func (k Keeper) GetTransferDirection(ctx sdk.Context, left, right uint64) []byte {
	store := ctx.KVStore(k.storeKey)
	ret := store.Get(types.GetTransferDirectionKey(left, right))
	return ret
}

// SetTransferDirection s
func (k Keeper) SetTransferDirection(ctx sdk.Context, left, right uint64, value []byte) {
	store := ctx.KVStore(k.storeKey)
	store.Set(types.GetTransferDirectionKey(left, right), value)
}

// CheckTransferDirectionIsRight c
func (k Keeper) CheckTransferDirectionIsRight(ctx sdk.Context, left, right uint64) bool {
	direction := k.GetTransferDirection(ctx, left, right)

	// if direction not exist before, set from left to right as right direction, and right to left as left direction,it will keep invoke GetTransferDirection not be nil
	if direction == nil {
		k.SetTransferDirection(ctx, left, right, types.TransferDirectionRight)
		k.SetTransferDirection(ctx, right, left, types.TransferDirectionLeft)
		return true
	}

	if bytes.Compare(direction, types.TransferDirectionRight) == 0 {
		return true
	} else if bytes.Compare(direction, types.TransferDirectionLeft) == 0 {
		return false
	} else {
		panic("Unknown direction types")
	}
}

// ReceiverChainDirectionIsRight r
func (k Keeper) ReceiverChainDirectionIsRight(ctx sdk.Context, sourceNamespaceId, targetNamespaceId uint64) bool {
	direction := k.GetTransferDirection(ctx, targetNamespaceId, sourceNamespaceId)

	// if direction not exist before, set from left to right as right direction, and right to left as left direction,it will keep invoke GetTransferDirection not be nil
	if direction == nil {
		panic("ReceiverChainDirection get the direction must be exist")
	}

	if bytes.Compare(direction, types.TransferDirectionRight) == 0 {
		return true
	} else if bytes.Compare(direction, types.TransferDirectionLeft) == 0 {
		return false
	} else {
		panic("Unknown direction types")
	}
}

// SendCoinToTheTargetAccount s
func (k Keeper) SendCoinToTheTargetAccount(ctx sdk.Context, coin sdk.Coin, recipientAddr sdk.AccAddress) error {
	// mint new tokens if the source of the transfer is the same chain
	if err := k.bankKeeper.MintCoins(
		ctx, types.ModuleName, sdk.NewCoins(coin),
	); err != nil {
		return err
	}

	// send to receiver
	if err := k.bankKeeper.SendCoinsFromModuleToAccount(
		ctx, types.ModuleName, recipientAddr, sdk.NewCoins(coin),
	); err != nil {
		return err
	}
	return nil
}

func (k Keeper) BlockedAddr(addr sdk.AccAddress) bool {
	return k.bankKeeper.BlockedAddr(addr)
}

// RefundPacketToken will unescrow and send back the tokens back to sender
// if the sending chain was the source chain. Otherwise, the sent tokens
// were burnt in the original send so new tokens are minted and sent to
// the sending address.
func (k Keeper) RefundPacketToken(ctx sdk.Context, acknowledgement *pibctypes.MsgAcknowledgement, packet types.Packet) error {

	// parse the transfer amount
	transferAmount := packet.Token.Amount

	token := sdk.NewCoin(packet.Token.Denom, transferAmount)

	// decode the sender address
	sender, err := sdk.AccAddressFromBech32(packet.Sender)
	if err != nil {
		return err
	}

	if k.CheckTransferDirectionIsRight(ContextWithNamespace(ctx, 0), acknowledgement.Packet.SourceNamespaceId, acknowledgement.Packet.TargetNamespaceId) {
		// unescrow tokens back to sender
		escrowAddress := types.GetEscrowAddress(acknowledgement.Packet.SourceNamespaceId, acknowledgement.Packet.TargetNamespaceId, token.Denom)
		return k.UnescrowToken(ctx, escrowAddress, sender, token)
	}

	// mint vouchers back to sender
	if err := k.bankKeeper.MintCoins(
		ctx, types.ModuleName, sdk.NewCoins(token),
	); err != nil {
		return err
	}

	if err := k.bankKeeper.SendCoinsFromModuleToAccount(ctx, types.ModuleName, sender, sdk.NewCoins(token)); err != nil {
		panic(fmt.Sprintf("unable to send coins from module to account despite previously minting coins to module account: %v", err))
	}

	return nil
}

// DecodePacket d
func (k Keeper) DecodePacket(packetData []byte) (*types.Packet, error) {
	var pd types.Packet
	if err := k.cdc.Unmarshal(packetData, &pd); err != nil {
		return nil, err
	}
	return &pd, nil
}

// GetTransactionState g
func (k Keeper) GetTransactionState(ctx sdk.Context, txId []byte) *types.TransactionState {
	store := ctx.KVStore(k.storeKey)
	cacheData := store.Get(types.GetTransactionPacketCacheKey(txId))
	if cacheData == nil {
		return nil
	}
	var ret types.TransactionState
	k.cdc.MustUnmarshal(cacheData, &ret)

	return &ret
}

// SetTransactionState s
func (k Keeper) SetTransactionState(ctx sdk.Context, txId []byte, cache types.TransactionState) {
	store := ctx.KVStore(k.storeKey)
	data := k.cdc.MustMarshal(&cache)
	store.Set(types.GetTransactionPacketCacheKey(txId), data)
}

// SetTransactionState s
func (k Keeper) SetTransactionModuleName(ctx sdk.Context, txId []byte) error {
	return k.pibckeeper.SetSourceTransactionModule(ctx, txId, types.ModuleName)
}
