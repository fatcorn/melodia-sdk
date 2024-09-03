package keeper

import (
	"context"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/types"
	"github.com/golang/protobuf/proto"
)

var _ types.MsgServer = Keeper{}

//type MsgServer struct {
//	Keeper
//}

func (m Keeper) RecvPacket(ctx context.Context, packet *types.MsgRecvPacket) (*types.MsgRecvPacketResponse, error) {

	sdkCtx := sdk.UnwrapSDKContext(ctx)

	sourceTransactionIdentifier := packet.TransactionIdentifier(nil)

	moduleName, err := m.GetSourceTransactionModule(sdkCtx, sourceTransactionIdentifier)
	if err != nil {
		return nil, types.ErrSourceTxIdNoteFind
	}
	module, hasRoute := m.Router.GetRoute(moduleName)
	if !hasRoute {
		return nil, types.ErrInvalidPIBCModule
	}

	sp, err := m.GetTransactionSourcePacket(sdkCtx, sourceTransactionIdentifier)
	if err != nil {
		return nil, err
	}

	if !proto.Equal(sp, packet.Packet) {
		return nil, fmt.Errorf("input args not match with before")
	}

	err = module.OnRecvPacket(ctx, packet)
	if err != nil {
		return nil, err
	}
	return &types.MsgRecvPacketResponse{}, nil
}

func (m Keeper) Acknowledgement(ctx context.Context, acknowledgement *types.MsgAcknowledgement) (*types.MsgAcknowledgementResponse, error) {
	sdkCtx := sdk.UnwrapSDKContext(ctx)

	sourceTransactionIdentifier := acknowledgement.TransactionIdentifier(nil)

	moduleName, err := m.GetSourceTransactionModule(sdkCtx, sourceTransactionIdentifier)
	if err != nil {
		return nil, types.ErrSourceTxIdNoteFind
	}
	module, hasRoute := m.Router.GetRoute(moduleName)
	if !hasRoute {
		return nil, types.ErrInvalidPIBCModule
	}

	sp, err := m.GetTransactionSourcePacket(sdkCtx, sourceTransactionIdentifier)
	if err != nil {
		return nil, err
	}

	if !proto.Equal(sp, acknowledgement.Packet) {
		return nil, fmt.Errorf("input args not match with before")
	}

	err = module.Acknowledgement(ctx, acknowledgement)
	if err != nil {
		return nil, err
	}
	return &types.MsgAcknowledgementResponse{}, nil
}
