package keeper

import (
	"context"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cosmos/cosmos-sdk/x/pibc/types"
)

var _ types.QueryServer = queryServer{}

func NewQueryServer(k *Keeper) types.QueryServer {
	return queryServer{k: *k}
}

type queryServer struct{ k Keeper }

// SourcePacket returns account details based on address
func (s queryServer) SourcePacket(ctx context.Context, req *types.QuerySourcePacketRequest) (*types.QuerySourcePacketResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "empty request")
	}

	if len(req.TxId) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Address cannot be empty")
	}

	sdkCtx := sdk.UnwrapSDKContext(ctx)

	packet, err := s.k.GetTransactionSourcePacket(sdkCtx, req.TxId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "txId %s not found", req.TxId)
	}
	if packet == nil {
		return nil, status.Errorf(codes.NotFound, "txId %s not found", req.TxId)
	}

	return &types.QuerySourcePacketResponse{BasePacket: packet}, nil
}
