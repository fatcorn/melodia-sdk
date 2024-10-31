package types

// SourceTransactionPacket the namespaceA tx executed，would save this object data to SourceTransactionPacketKey
type SourceTransactionPacket struct {
	ModuleName        string
	SourceNamespaceId string
	TargetNamespaceId string
	PacketData        []byte
}
