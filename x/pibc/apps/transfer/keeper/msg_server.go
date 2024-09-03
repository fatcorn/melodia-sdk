package keeper

import (
	"context"
	sdkerrors "cosmossdk.io/errors"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/apps/transfer/types"
	pibctypes "github.com/cosmos/cosmos-sdk/x/pibc/types"
)

var _ types.MsgServer = Keeper{}

// NewMsgServerImpl returns an implementation of the MsgServer interface
// for the provided Keeper.
func NewMsgServerImpl(keeper Keeper) types.MsgServer {
	return &keeper
}

// Transfer defines an rpc handler method for MsgTransfer.
func (k Keeper) Transfer(goCtx context.Context, msg *types.MsgTransfer) (*types.MsgTransferResponse, error) {
	sdkCtx := sdk.UnwrapSDKContext(goCtx)

	txId := msg.TransactionIdentifier(sdkCtx.TxBytes())

	if k.GetTransactionState(sdkCtx, txId) != nil {
		return nil, fmt.Errorf("repeat pibc transfer tx")
	}

	//if !k.GetSendEnabled(sdkCtx) {
	//	return nil, types.ErrSendDisabled
	//}

	sender, err := sdk.AccAddressFromBech32(msg.Sender)
	if err != nil {
		return nil, err
	}

	if !k.bankKeeper.IsSendEnabledCoin(goCtx, msg.Token) {
		return nil, sdkerrors.Wrapf(types.ErrSendDisabled, "%s transfers are currently disabled", msg.Token.Denom)
	}

	if k.bankKeeper.BlockedAddr(sender) {
		return nil, sdkerrors.Wrapf(types.ErrUnauthorized, "%s is not allowed to send funds", sender)
	}

	if k.CheckTransferDirectionIsRight(ContextWithNamespace(goCtx, 0), msg.SourceNamespaceId, msg.TargetNamespaceId) {
		//labels = append(labels, telemetry.NewLabel(coretypes.LabelSource, "true"))

		// obtain the escrow address for the source channel end
		escrowAddress := types.GetEscrowAddress(msg.SourceNamespaceId, msg.TargetNamespaceId, msg.Token.Denom)

		if err := k.escrowToken(ContextWithNamespace(goCtx, msg.SourceNamespaceId), sender, escrowAddress, msg.Token); err != nil {
			return nil, err
		}

	} else {
		//labels = append(labels, telemetry.NewLabel(coretypes.LabelSource, "false"))

		// transfer the coins to the module account and burn them
		if err := k.bankKeeper.SendCoinsFromAccountToModule(
			ContextWithNamespace(goCtx, msg.SourceNamespaceId), sender, types.ModuleName, sdk.NewCoins(msg.Token),
		); err != nil {
			return nil, err
		}

		if err := k.bankKeeper.BurnCoins(
			ContextWithNamespace(goCtx, msg.SourceNamespaceId), types.ModuleName, sdk.NewCoins(msg.Token),
		); err != nil {
			// NOTE: should not happen as the module account was
			// retrieved on the step above and it has enough balace
			// to burn.
			panic(fmt.Sprintf("cannot burn coins after a successful send to a module account: %v", err))
		}
	}
	err = k.SetTransactionModuleName(sdkCtx, txId)
	if err != nil {
		return nil, err
	}

	packet := types.Packet{
		Token:    msg.Token,
		Sender:   msg.Sender,
		Receiver: msg.Receiver,
	}

	packetData := k.cdc.MustMarshal(&packet)

	sourcePacket := pibctypes.BasePacket{
		SourceNamespaceId: msg.SourceNamespaceId,
		TargetNamespaceId: msg.TargetNamespaceId,
		Data:              packetData,
	}

	err = k.pibckeeper.SetTransactionSourcePacket(sdkCtx, txId, &sourcePacket)

	if err != nil {
		return nil, err
	}

	txState := types.TransactionState{
		State: 0 + 1, // this tx execute success
	}
	k.SetTransactionState(sdkCtx, txId, txState)

	fmt.Printf("-----------------pibc transfer successed \n")
	return &types.MsgTransferResponse{}, nil
}
