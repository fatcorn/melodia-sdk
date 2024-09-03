package transfer

import (
	"context"
	sdkerrors "cosmossdk.io/errors"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/apps/transfer/keeper"
	"github.com/cosmos/cosmos-sdk/x/pibc/apps/transfer/types"
	pibctypes "github.com/cosmos/cosmos-sdk/x/pibc/types"
)

var (
	_ pibctypes.IsCrossNamespaceModule = (*PIBCModule)(nil)
	//_ porttypes.PacketDataUnmarshaler = (*IBCModule)(nil)

)

// NewPIBCModule creates a new PIBCModule given the keeper
func NewPIBCModule(k keeper.Keeper) PIBCModule {
	return PIBCModule{
		keeper: k,
	}
}

// PIBCModule implements the ICS26 interface for transfer given the transfer keeper.
type PIBCModule struct {
	keeper keeper.Keeper
}

func (pm PIBCModule) GetTransactionsMap() map[string]string {
	//TODO implement me
	panic("implement me")
}

func (pm PIBCModule) OnRecvPacket(ctx context.Context, msg *pibctypes.MsgRecvPacket) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)

	cache := pm.keeper.GetTransactionState(sdkCtx, msg.TransactionIdentifier(nil))
	if cache == nil || cache.State < types.TransferInTransactionStateSuccess {
		return fmt.Errorf("the cross namespace transfer tx not executed or executed failed")
	}

	pd, err := pm.keeper.DecodePacket(msg.Packet.Data)
	if err != nil {
		return err
	}
	//
	//cacheFromArgs := types.MsgTransfer{
	//	SourceNamespaceId: msg.Packet.SourceNamespaceId,
	//	TargetNamespaceId: msg.Packet.TargetNamespaceId,
	//	Token:             pd.Token,
	//	Sender:            pd.Sender,
	//	Receiver:          pd.Receiver,
	//}
	//
	//if !proto.Equal(cache.TransactionArgsInfo, &cacheFromArgs) {
	//	return fmt.Errorf("input args not match with before")
	//}

	// decode the receiver address
	receiver, err := sdk.AccAddressFromBech32(pd.Receiver)
	if err != nil {
		return err
	}

	// parse the transfer amount
	transferAmount := pd.Token.Amount

	//labels := []metrics.Label{
	//	telemetry.NewLabel(coretypes.LabelSourcePort, packet.GetSourcePort()),
	//	telemetry.NewLabel(coretypes.LabelSourceChannel, packet.GetSourceChannel()),
	//}

	defer func() {
		pm.keeper.SetTransactionState(sdkCtx, msg.TransactionIdentifier(nil), *cache)
		// todo delete cache and the SourcePacket
	}()

	// This is the prefix that would have been prefixed to the denomination
	// on sender chain IF and only if the token originally came from the
	// receiving chain.
	//
	// NOTE: We use SourcePort and SourceChannel here, because the counterparty
	// chain would have prefixed with DestPort and DestChannel when originally
	// receiving this coin as seen in the "sender chain is the source" condition.
	if pm.keeper.ReceiverChainDirectionIsRight(keeper.ContextWithNamespace(ctx, 0), msg.Packet.SourceNamespaceId, msg.Packet.TargetNamespaceId) {
		// sender chain is not the source, unescrow tokens

		// remove prefix added by sender chain
		voucherPrefix := types.GetDenomPrefix(msg.Packet.TargetNamespaceId)
		denom := pd.Token.Denom[len(voucherPrefix):]

		token := sdk.NewCoin(denom, transferAmount)

		if pm.keeper.BlockedAddr(receiver) {
			return sdkerrors.Wrapf(types.ErrUnauthorized, "%s is not allowed to receive funds", receiver)
		}

		escrowAddress := types.GetEscrowAddress(msg.Packet.TargetNamespaceId, msg.Packet.SourceNamespaceId, denom)
		if err := pm.keeper.UnescrowToken(keeper.ContextWithNamespace(ctx, msg.Packet.TargetNamespaceId), escrowAddress, receiver, token); err != nil {
			return err
		}

		//defer func() {
		//	if transferAmount.IsInt64() {
		//		telemetry.SetGaugeWithLabels(
		//			[]string{"ibc", types.ModuleName, "packet", "receive"},
		//			float32(transferAmount.Int64()),
		//			[]metrics.Label{telemetry.NewLabel(coretypes.LabelDenom, unprefixedDenom)},
		//		)
		//	}
		//
		//	telemetry.IncrCounterWithLabels(
		//		[]string{"ibc", types.ModuleName, "receive"},
		//		1,
		//		append(
		//			labels, telemetry.NewLabel(coretypes.LabelSource, "true"),
		//		),
		//	)
		//}()
		cache.State += 1
		return nil
	}

	// sender chain is the source, mint vouchers

	// since SendPacket did not prefix the denomination, we must prefix denomination here
	voucherDenom := types.GetPrefixedDenom(msg.Packet.SourceNamespaceId, pd.Token.Denom)

	voucher := sdk.NewCoin(voucherDenom, transferAmount)

	err = pm.keeper.SendCoinToTheTargetAccount(keeper.ContextWithNamespace(ctx, msg.Packet.TargetNamespaceId), voucher, receiver)
	if err != nil {
		return err
	}
	cache.State += 1

	return nil
}

func (pm PIBCModule) Acknowledgement(ctx context.Context, msg *pibctypes.MsgAcknowledgement) error {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	pd, err := pm.keeper.DecodePacket(msg.Packet.Data)
	if err != nil {
		return err
	}

	cache := pm.keeper.GetTransactionState(sdkCtx, msg.TransactionIdentifier(nil))
	if cache == nil || cache.State < types.TransferInTransactionStateSuccess {
		return fmt.Errorf("the cross namespace transfer tx not executed or executed failed")
	}

	if cache.State != types.TransferInTransactionStateSuccess {
		return fmt.Errorf("state not expected")
	}

	return pm.keeper.RefundPacketToken(keeper.ContextWithNamespace(ctx, msg.Packet.SourceNamespaceId), msg, *pd)
}

func (pm PIBCModule) GeneratePacketMsg(originMsg interface{}) *pibctypes.MsgRecvPacket {
	if transferMsg, ok := originMsg.(types.MsgTransfer); ok {
		packet := types.Packet{
			Token:    transferMsg.Token,
			Sender:   transferMsg.Sender,
			Receiver: transferMsg.Receiver,
		}
		packetData, err := packet.Marshal()
		if err != nil {
			return nil
		}

		sourcePacket := pibctypes.BasePacket{
			SourceNamespaceId: transferMsg.SourceNamespaceId,
			TargetNamespaceId: transferMsg.TargetNamespaceId,
			Data:              packetData,
		}
		return &pibctypes.MsgRecvPacket{
			Packet: &sourcePacket,
		}
	} else {
		return nil
	}
}
