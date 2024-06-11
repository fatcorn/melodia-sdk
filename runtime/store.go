package runtime

import (
	"context"
	"io"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/core/store"
	storetypes "cosmossdk.io/store/types"

	sdk "github.com/cosmos/cosmos-sdk/types"

	"encoding/binary"
)

func NewKVStoreService(storeKey *storetypes.KVStoreKey) store.KVStoreService {
	return &kvStoreService{key: storeKey}
}

type kvStoreService struct {
	key *storetypes.KVStoreKey
}

func (k kvStoreService) OpenKVStore(ctx context.Context) store.KVStore {
	return newKVStore(sdk.UnwrapSDKContext(ctx).KVStore(k.key))
}

func NewNamespaceKVStoreService(storeKey *storetypes.KVStoreKey) store.NamespaceKVStoreService {
	return &namespaceKvStoreService{key: storeKey}
}

type namespaceKvStoreService struct {
	key *storetypes.KVStoreKey
}

func (n namespaceKvStoreService) OpenNamespaceKVStore(ctx context.Context) store.KVStore {
	sdkCtx := sdk.UnwrapSDKContext(ctx)
	return newNamespaceCoreKVStore(sdkCtx.KVStore(n.key), sdkCtx.Namespace())
}

type memStoreService struct {
	key *storetypes.MemoryStoreKey
}

func (m memStoreService) OpenMemoryStore(ctx context.Context) store.KVStore {
	return newKVStore(sdk.UnwrapSDKContext(ctx).KVStore(m.key))
}

type transientStoreService struct {
	key *storetypes.TransientStoreKey
}

func (t transientStoreService) OpenTransientStore(ctx context.Context) store.KVStore {
	return newKVStore(sdk.UnwrapSDKContext(ctx).KVStore(t.key))
}

// CoreKVStore is a wrapper of Core/Store kvstore interface
// Remove after https://github.com/cosmos/cosmos-sdk/issues/14714 is closed
type coreKVStore struct {
	kvStore storetypes.KVStore
}

// NewKVStore returns a wrapper of Core/Store kvstore interface
// Remove once store migrates to core/store kvstore interface
func newKVStore(store storetypes.KVStore) store.KVStore {
	return coreKVStore{kvStore: store}
}

// Get returns nil iff key doesn't exist. Errors on nil key.
func (store coreKVStore) Get(key []byte) ([]byte, error) {
	return store.kvStore.Get(key), nil
}

// Has checks if a key exists. Errors on nil key.
func (store coreKVStore) Has(key []byte) (bool, error) {
	return store.kvStore.Has(key), nil
}

// Set sets the key. Errors on nil key or value.
func (store coreKVStore) Set(key, value []byte) error {
	store.kvStore.Set(key, value)
	return nil
}

// Delete deletes the key. Errors on nil key.
func (store coreKVStore) Delete(key []byte) error {
	store.kvStore.Delete(key)
	return nil
}

// Iterator iterates over a domain of keys in ascending order. End is exclusive.
// Start must be less than end, or the Iterator is invalid.
// Iterator must be closed by caller.
// To iterate over entire domain, use store.Iterator(nil, nil)
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// Exceptionally allowed for cachekv.Store, safe to write in the modules.
func (store coreKVStore) Iterator(start, end []byte) (store.Iterator, error) {
	return store.kvStore.Iterator(start, end), nil
}

// ReverseIterator iterates over a domain of keys in descending order. End is exclusive.
// Start must be less than end, or the Iterator is invalid.
// Iterator must be closed by caller.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// Exceptionally allowed for cachekv.Store, safe to write in the modules.
func (store coreKVStore) ReverseIterator(start, end []byte) (store.Iterator, error) {
	return store.kvStore.ReverseIterator(start, end), nil
}

// NamespaceCoreKVStore is a wrapper of Core/Store kvstore interface for namespace
// Remove after https://github.com/cosmos/cosmos-sdk/issues/14714 is closed
type NamespaceCoreKVStore struct {
	kvStore     storetypes.KVStore
	namespaceId uint64
}

// NewKVStore returns a wrapper of Core/Store kvstore interface
// Remove once store migrates to core/store kvstore interface
func newNamespaceCoreKVStore(store storetypes.KVStore, namespaceId uint64) store.KVStore {
	return NamespaceCoreKVStore{kvStore: store, namespaceId: namespaceId}
}

// Get returns nil iff key doesn't exist. Errors on nil key.
func (store NamespaceCoreKVStore) Get(key []byte) ([]byte, error) {
	indeedKey := store.buildIndeedKey(key)
	return store.kvStore.Get(indeedKey), nil
}

// Has checks if a key exists. Errors on nil key.
func (store NamespaceCoreKVStore) Has(key []byte) (bool, error) {
	indeedKey := store.buildIndeedKey(key)
	return store.kvStore.Has(indeedKey), nil
}

// Set sets the key. Errors on nil key or value.
func (store NamespaceCoreKVStore) Set(key, value []byte) error {
	indeedKey := store.buildIndeedKey(key)
	store.kvStore.Set(indeedKey, value)
	return nil
}

// Delete deletes the key. Errors on nil key.
func (store NamespaceCoreKVStore) Delete(key []byte) error {
	indeedKey := store.buildIndeedKey(key)
	store.kvStore.Delete(indeedKey)
	return nil
}

// Iterator iterates over a domain of keys in ascending order. End is exclusive.
// Start must be less than end, or the Iterator is invalid.
// Iterator must be closed by caller.
// To iterate over entire domain, use store.Iterator(nil, nil)
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// Exceptionally allowed for cachekv.Store, safe to write in the modules.
func (store NamespaceCoreKVStore) Iterator(start, end []byte) (store.Iterator, error) {
	startIndeedKey := store.buildIndeedKey(start)
	endIndeedKey := store.buildIndeedKey(end)
	return store.kvStore.Iterator(startIndeedKey, endIndeedKey), nil
}

// ReverseIterator iterates over a domain of keys in descending order. End is exclusive.
// Start must be less than end, or the Iterator is invalid.
// Iterator must be closed by caller.
// CONTRACT: No writes may happen within a domain while an iterator exists over it.
// Exceptionally allowed for cachekv.Store, safe to write in the modules.
func (store NamespaceCoreKVStore) ReverseIterator(start, end []byte) (store.Iterator, error) {
	startIndeedKey := store.buildIndeedKey(start)
	endIndeedKey := store.buildIndeedKey(end)
	return store.kvStore.ReverseIterator(startIndeedKey, endIndeedKey), nil
}

func (nsotre NamespaceCoreKVStore) buildIndeedKey(key []byte) []byte {
	namespacePrefixIndex := binary.LittleEndian.AppendUint64([]byte{}, nsotre.namespaceId)
	indeedKey := append(namespacePrefixIndex, key...)
	return indeedKey
}

// Adapter
var _ storetypes.KVStore = kvStoreAdapter{}

type kvStoreAdapter struct {
	store store.KVStore
}

func (kvStoreAdapter) CacheWrap() storetypes.CacheWrap {
	panic("unimplemented")
}

func (kvStoreAdapter) CacheWrapWithTrace(w io.Writer, tc storetypes.TraceContext) storetypes.CacheWrap {
	panic("unimplemented")
}

func (kvStoreAdapter) GetStoreType() storetypes.StoreType {
	panic("unimplemented")
}

func (s kvStoreAdapter) Delete(key []byte) {
	err := s.store.Delete(key)
	if err != nil {
		panic(err)
	}
}

func (s kvStoreAdapter) Get(key []byte) []byte {
	bz, err := s.store.Get(key)
	if err != nil {
		panic(err)
	}
	return bz
}

func (s kvStoreAdapter) Has(key []byte) bool {
	has, err := s.store.Has(key)
	if err != nil {
		panic(err)
	}
	return has
}

func (s kvStoreAdapter) Set(key, value []byte) {
	err := s.store.Set(key, value)
	if err != nil {
		panic(err)
	}
}

func (s kvStoreAdapter) Iterator(start, end []byte) dbm.Iterator {
	it, err := s.store.Iterator(start, end)
	if err != nil {
		panic(err)
	}
	return it
}

func (s kvStoreAdapter) ReverseIterator(start, end []byte) dbm.Iterator {
	it, err := s.store.ReverseIterator(start, end)
	if err != nil {
		panic(err)
	}
	return it
}

func KVStoreAdapter(store store.KVStore) storetypes.KVStore {
	return &kvStoreAdapter{store}
}
