package types

import (
	"context"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

type IsCrossNamespaceModule interface {
	sdk.PIBCPacketGenerator
	// GetTransactionsMap get app transactions map
	GetTransactionsMap() map[string]string

	// OnRecvPacket defines a rpc handler method for MsgRecvPacket
	OnRecvPacket(context.Context, *MsgRecvPacket) error

	// Acknowledgement defines a rpc handler method for MsgAcknowledgement
	Acknowledgement(context.Context, *MsgAcknowledgement) error

	//// GeneratePacketMsg , generate MsgRecvPacket by origin msg executed by namespace A
	//GeneratePacketMsg(ctx context.Context, tx sdk.Tx) []sdk.Msg
}

// CrossNamespaceTx cross namespace module app's tx should implement this interface
type CrossNamespaceTx interface {
	// TransactionIdentifier recomad use
	TransactionIdentifier(txPlain []byte) []byte // transaction identifier, to identify transactional txs in same transaction
}

// CrossNamespaceTransactionalTx cross namespace module app's transactional tx should implement this interface
type CrossNamespaceTransactionalTx interface {
	CrossNamespaceTx

	TransactionName() []byte // get transaction key
}
