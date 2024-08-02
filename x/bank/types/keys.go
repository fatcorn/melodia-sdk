package types

import (
	"cosmossdk.io/collections"
	collcodec "cosmossdk.io/collections/codec"
	"cosmossdk.io/math"
	"github.com/cosmos/cosmos-sdk/types/address"
	"github.com/cosmos/cosmos-sdk/types/kv"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

const (
	// ModuleName defines the module name
	ModuleName = "bank"

	// StoreKey defines the primary module store key
	StoreKey = ModuleName

	MemoryKey = "memory:" + ModuleName

	// RouterKey defines the module's message routing key
	RouterKey = ModuleName
)

// KVStore keys
var (
	SupplyKey           = collections.NewPrefix(0)
	DenomMetadataPrefix = collections.NewPrefix(1)
	// BalancesPrefix is the prefix for the account balances store. We use a byte
	// (instead of `[]byte("balances")` to save some disk space).
	BalancesPrefix     = collections.NewPrefix(2)
	DenomAddressPrefix = collections.NewPrefix(3)
	// SendEnabledPrefix is the prefix for the SendDisabled flags for a Denom.
	SendEnabledPrefix = collections.NewPrefix(4)

	// ParamsKey is the prefix for x/bank parameters
	ParamsKey = collections.NewPrefix(5)

	// Defered
	DeferredCachePrefix = []byte{0x06}
)

// BalanceValueCodec is a codec for encoding bank balances in a backwards compatible way.
// Historically, balances were represented as Coin, now they're represented as a simple math.Int
var BalanceValueCodec = collcodec.NewAltValueCodec(sdk.IntValue, func(bytes []byte) (math.Int, error) {
	c := new(sdk.Coin)
	err := c.Unmarshal(bytes)
	if err != nil {
		return math.Int{}, err
	}
	return c.Amount, nil
})

// Deferred

// This creates the prefix for use for the mem KV store used to track deferred balances by module name
func CreateDeferredCacheModulePrefix(moduleAddr []byte) []byte {
	return append(DeferredCachePrefix, address.MustLengthPrefix(moduleAddr)...)
}

// This creates the prefix for use for the mem KV store used to track deferred balances by module and txIndex to appropriately partition reads and writes to and from module balances
func CreateDeferredCacheModuleTxIndexedPrefix(moduleAddr []byte, index uint64) []byte {
	return append(CreateDeferredCacheModulePrefix(moduleAddr), sdk.Uint64ToBigEndian(index)...)
}

// AddressFromDeferredCacheStore returns an account address from a deferred Cache prefix
// store. The key must not contain the prefix DeferredCachePrefix as the prefix store
// iterator discards the actual prefix.
//
// If invalid key is passed, AddressFromBalancesStore returns ErrInvalidKey.
func AddressFromDeferredCacheStore(key []byte) (sdk.AccAddress, error) {
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}
	kv.AssertKeyAtLeastLength(key, 1)
	addrLen := key[0]
	bound := int(addrLen)
	if len(key)-1 < bound {
		return nil, ErrInvalidKey
	}
	return key[1 : bound+1], nil
}
