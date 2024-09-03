package types

import (
	"context"
)

type IsCrossNamespaceModule interface {
	// GetTransactionsMap get app transactions map
	GetTransactionsMap() map[string]string

	// OnRecvPacket defines a rpc handler method for MsgRecvPacket
	OnRecvPacket(context.Context, *MsgRecvPacket) error

	// Acknowledgement defines a rpc handler method for MsgAcknowledgement
	Acknowledgement(context.Context, *MsgAcknowledgement) error

	// GeneratePacketMsg , generate MsgRecvPacket by origin msg executed by namespace A
	GeneratePacketMsg(originMsg interface{}) *MsgRecvPacket
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
