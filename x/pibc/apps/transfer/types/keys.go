package types

import (
	"crypto/sha256"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

const (
	// ModuleName defines the module name
	ModuleName = "pibc_transfer"

	// StoreKey defines the primary module store key
	StoreKey = ModuleName

	// MemStoreKey defines the in-memory store key
	MemStoreKey = "mem_keys.go"

	KeyTotalEscrowPrefix = "totalEscrowForDenom"

	EscrowAddressPrefix = "escrow_address_"
)

var (
	ParamsKey                      = []byte("p_pibc_transfer")
	SourceTransactionPacketBaseKey = []byte("source_transaction_packet")
	TransferDirectionBaseKey       = []byte("direction_base")

	TransactionPacketCachePrefixKey = []byte("transaction_packet_cache")
)

func SourceTransactionPacketKey(sourceTxId []byte) []byte {
	return append(SourceTransactionPacketBaseKey, sourceTxId...)
}

// TotalEscrowForDenomKey returns the store key of under which the total amout of
// source chain tokens in escrow is stored.
func TotalEscrowForDenomKey(denom string) []byte {
	return []byte(fmt.Sprintf("%s/%s", KeyTotalEscrowPrefix, denom))
}

func GetEscrowAddress(sourceNamespaceId, targetNamespaceId uint64, token string) sdk.AccAddress {
	// a slash is used to create domain separation between port and channel identifiers to
	// prevent address collisions between escrow addresses created for different channels
	contents := fmt.Sprintf("%v/%v/%s", sourceNamespaceId, targetNamespaceId, token)

	// ADR 028 AddressHash construction
	preImage := []byte(EscrowAddressPrefix)
	preImage = append(preImage, 0)
	preImage = append(preImage, contents...)
	hash := sha256.Sum256(preImage)
	return hash[:20]
}

func GetTransferDirectionKey(left, right uint64) []byte {
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	return append(append(TransferDirectionBaseKey, []byte(leftStr)...), []byte(rightStr)...)
}

//func GetTransactionStateKey(txId []byte) []byte {
//	return append(append([]byte(ModuleName), TransactionStatePrefixKey...), txId...)
//}

func GetTransactionPacketCacheKey(txId []byte) []byte {
	return append(append([]byte(ModuleName), TransactionPacketCachePrefixKey...), txId...)
}
