package types

func (msgPacket *MsgRecvPacket) TransactionName() []byte {
	return []byte{0x1}
}

func (msgPacket *MsgRecvPacket) TransactionIdentifier([]byte) []byte {
	return msgPacket.SourcePacketTxId
}

func (msgPacket *MsgAcknowledgement) TransactionIdentifier([]byte) []byte {
	return msgPacket.SourcePacketTxId
}
