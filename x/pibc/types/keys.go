package types

const (
	// ModuleName defines the module name
	ModuleName = "pibc"

	// StoreKey defines the primary module store key
	StoreKey = ModuleName

	// MemStoreKey defines the in-memory store key
	MemStoreKey = "mem_keys.go"
)

var (
	ParamsKey                      = []byte("p_pibc")
	SourceTransactionModuleBaseKey = []byte("source_transaction_module")

	TransactionSourcePacketBaseKey = []byte("transaction_source_packet")
)

func GetSourceTransactionModuleKey(sourceTxId []byte) []byte {
	return append(SourceTransactionModuleBaseKey, sourceTxId...)
}

func GetTransactionSourcePacketKey(sourceTxId []byte) []byte {
	return append(TransactionSourcePacketBaseKey, sourceTxId...)
}
