package cachekv

import (
	"bytes"
	"io"
	"sort"
	"sync"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/cachekv/internal"
	"cosmossdk.io/store/internal/conv"
	"cosmossdk.io/store/internal/kv"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"
)

// cValue represents a cached value.
// If dirty is true, it indicates the cached value is different from the underlying value.
type cValue struct {
	value []byte
	dirty bool
}

// Store wraps an in-memory cache around an underlying types.KVStore.
type Store struct {
	//mtx           sync.Mutex
	//cache         map[string]*cValue
	//unsortedCache map[string]struct{}
	mtx           sync.RWMutex
	cache         *sync.Map
	unsortedCache *sync.Map
	sortedCache   internal.BTree // always ascending sorted
	parent        types.KVStore
}

var _ types.CacheKVStore = (*Store)(nil)

// NewStore creates a new Store object
func NewStore(parent types.KVStore) *Store {
	return &Store{
		//cache:         make(map[string]*cValue),
		//unsortedCache: make(map[string]struct{}),
		cache:         &sync.Map{},
		unsortedCache: &sync.Map{},
		sortedCache:   internal.NewBTree(),
		parent:        parent,
	}
}

// GetStoreType implements Store.
func (store *Store) GetStoreType() types.StoreType {
	return store.parent.GetStoreType()
}

// getFromCache queries the write-through cache for a value by key.
func (store *Store) getFromCache(key []byte) []byte {
	if cv, ok := store.cache.Load(conv.UnsafeBytesToStr(key)); ok {
		return cv.(*cValue).value
	}
	return store.parent.Get(key)
}

// Get implements types.KVStore.
func (store *Store) Get(key []byte) (value []byte) {
	types.AssertValidKey(key)
	return store.getFromCache(key)
}

// Set implements types.KVStore.
func (store *Store) Set(key, value []byte) {
	types.AssertValidKey(key)
	types.AssertValidValue(value)

	store.setCacheValue(key, value, true)
}

// Has implements types.KVStore.
func (store *Store) Has(key []byte) bool {
	value := store.Get(key)
	return value != nil
}

// Delete implements types.KVStore.
func (store *Store) Delete(key []byte) {
	types.AssertValidKey(key)

	store.setCacheValue(key, nil, true)
}

func (store *Store) resetCaches() {
	store.cache = &sync.Map{}
	store.unsortedCache = &sync.Map{}
	store.sortedCache = internal.NewBTree()
}

// Implements Cachetypes.KVStore.
func (store *Store) Write() {
	store.mtx.Lock()
	defer store.mtx.Unlock()

	type cEntry struct {
		key string
		val *cValue
	}

	sortedCache := make([]cEntry, 0)

	store.cache.Range(func(key, value any) bool {
		if value.(*cValue).dirty {
			sortedCache = append(sortedCache, cEntry{
				key: key.(string),
				val: value.(*cValue),
			})
		}
		return true
	})

	// Iterate unsortedCache, if unsortedCache has more than 1 item, break.
	unsortedCacheSize := 0
	store.unsortedCache.Range(func(key, value any) bool {
		unsortedCacheSize++
		if unsortedCacheSize > 1 {
			return false
		}
		return true
	})

	if len(sortedCache) == 0 && unsortedCacheSize == 0 {
		store.sortedCache = internal.NewBTree()
		return
	}

	// We need a copy of all of the keys.
	// Not the best. To reduce RAM pressure, we copy the values as well
	// and clear out the old caches right after the copy.
	//sortedCache := make([]cEntry, 0, len(keys))
	//
	//for key, dbValue := range store.cache {
	//	if dbValue.dirty {
	//		sortedCache = append(sortedCache, cEntry{key, dbValue})
	//	}
	//}
	store.resetCaches()
	sort.Slice(sortedCache, func(i, j int) bool {
		return sortedCache[i].key < sortedCache[j].key
	})

	// TODO: Consider allowing usage of Batch, which would allow the write to
	// at least happen atomically.
	for _, obj := range sortedCache {
		// We use []byte(key) instead of conv.UnsafeStrToBytes because we cannot
		// be sure if the underlying store might do a save with the byteslice or
		// not. Once we get confirmation that .Delete is guaranteed not to
		// save the byteslice, then we can assume only a read-only copy is sufficient.
		if obj.val.value != nil {
			// It already exists in the parent, hence update it.
			store.parent.Set([]byte(obj.key), obj.val.value)
		} else {
			store.parent.Delete([]byte(obj.key))
		}
	}
}

// CacheWrap implements CacheWrapper.
func (store *Store) CacheWrap() types.CacheWrap {
	return NewStore(store)
}

// CacheWrapWithTrace implements the CacheWrapper interface.
func (store *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return NewStore(tracekv.NewStore(store, w, tc))
}

//----------------------------------------
// Iteration

// Iterator implements types.KVStore.
func (store *Store) Iterator(start, end []byte) types.Iterator {
	return store.iterator(start, end, true)
}

// ReverseIterator implements types.KVStore.
func (store *Store) ReverseIterator(start, end []byte) types.Iterator {
	return store.iterator(start, end, false)
}

func (store *Store) iterator(start, end []byte, ascending bool) types.Iterator {
	store.mtx.Lock()
	defer store.mtx.Unlock()

	store.dirtyItems(start, end)
	isoSortedCache := store.sortedCache.Copy()

	var (
		err           error
		parent, cache types.Iterator
	)

	if ascending {
		parent = store.parent.Iterator(start, end)
		cache, err = isoSortedCache.Iterator(start, end)
	} else {
		parent = store.parent.ReverseIterator(start, end)
		cache, err = isoSortedCache.ReverseIterator(start, end)
	}
	if err != nil {
		panic(err)
	}

	return internal.NewCacheMergeIterator(parent, cache, ascending)
}

func findStartIndex(strL []string, startQ string) int {
	// Modified binary search to find the very first element in >=startQ.
	if len(strL) == 0 {
		return -1
	}

	var left, right, mid int
	right = len(strL) - 1
	for left <= right {
		mid = (left + right) >> 1
		midStr := strL[mid]
		if midStr == startQ {
			// Handle condition where there might be multiple values equal to startQ.
			// We are looking for the very first value < midStL, that i+1 will be the first
			// element >= midStr.
			for i := mid - 1; i >= 0; i-- {
				if strL[i] != midStr {
					return i + 1
				}
			}
			return 0
		}
		if midStr < startQ {
			left = mid + 1
		} else { // midStrL > startQ
			right = mid - 1
		}
	}
	if left >= 0 && left < len(strL) && strL[left] >= startQ {
		return left
	}
	return -1
}

func findEndIndex(strL []string, endQ string) int {
	if len(strL) == 0 {
		return -1
	}

	// Modified binary search to find the very first element <endQ.
	var left, right, mid int
	right = len(strL) - 1
	for left <= right {
		mid = (left + right) >> 1
		midStr := strL[mid]
		if midStr == endQ {
			// Handle condition where there might be multiple values equal to startQ.
			// We are looking for the very first value < midStL, that i+1 will be the first
			// element >= midStr.
			for i := mid - 1; i >= 0; i-- {
				if strL[i] < midStr {
					return i + 1
				}
			}
			return 0
		}
		if midStr < endQ {
			left = mid + 1
		} else { // midStrL > startQ
			right = mid - 1
		}
	}

	// Binary search failed, now let's find a value less than endQ.
	for i := right; i >= 0; i-- {
		if strL[i] < endQ {
			return i
		}
	}

	return -1
}

type sortState int

const (
	stateUnsorted sortState = iota
	stateAlreadySorted
)

const minSortSize = 1024

// Constructs a slice of dirty items, to use w/ memIterator.
func (store *Store) dirtyItems(start, end []byte) {
	startStr, endStr := conv.UnsafeBytesToStr(start), conv.UnsafeBytesToStr(end)
	if end != nil && startStr > endStr {
		// Nothing to do here.
		return
	}

	unsorted := make([]*kv.Pair, 0)
	store.unsortedCache.Range(func(key, value any) bool {
		cKey := key.(string)
		if dbm.IsKeyInDomain(conv.UnsafeStrToBytes(cKey), start, end) {
			cacheValue, ok := store.cache.Load(key)
			if ok {
				unsorted = append(unsorted, &kv.Pair{Key: []byte(cKey), Value: cacheValue.(*cValue).value})
			}
		}
		return true
	})
	store.clearUnsortedCacheSubset(unsorted, stateAlreadySorted)
}

func (store *Store) clearUnsortedCacheSubset(unsorted []*kv.Pair, sortState sortState) {

	store.deleteKeysFromUnsortedCache(unsorted)

	if sortState == stateUnsorted {
		sort.Slice(unsorted, func(i, j int) bool {
			return bytes.Compare(unsorted[i].Key, unsorted[j].Key) < 0
		})
	}

	for _, item := range unsorted {
		// sortedCache is able to store `nil` value to represent deleted items.
		store.sortedCache.Set(item.Key, item.Value)
	}
}

func (store *Store) deleteKeysFromUnsortedCache(unsorted []*kv.Pair) {
	for _, kv := range unsorted {
		keyStr := conv.UnsafeBytesToStr(kv.Key)
		store.unsortedCache.Delete(keyStr)
	}
}

//----------------------------------------
// etc

// Only entrypoint to mutate store.cache.
// A `nil` value means a deletion.
func (store *Store) setCacheValue(key, value []byte, dirty bool) {
	keyStr := conv.UnsafeBytesToStr(key)
	//store.cache[keyStr] = &cValue{
	//	value: value,
	//	dirty: dirty,
	//}
	store.cache.Store(keyStr, &cValue{
		value: value,
		dirty: dirty,
	})
	if dirty {
		//store.unsortedCache[keyStr] = struct{}{}
		store.unsortedCache.Store(keyStr, struct{}{})
	}
}

func (store *Store) GetParent() types.KVStore {
	return store.parent
}

func (store *Store) DeleteAll(start, end []byte) error {
	for _, k := range store.GetAllKeyStrsInRange(start, end) {
		store.Delete([]byte(k))
	}
	return nil
}

func (store *Store) GetAllKeyStrsInRange(start, end []byte) (res []string) {
	keyStrs := map[string]struct{}{}
	for _, pk := range store.parent.GetAllKeyStrsInRange(start, end) {
		keyStrs[pk] = struct{}{}
	}
	store.cache.Range(func(key, value any) bool {
		kbz := []byte(key.(string))
		if bytes.Compare(kbz, start) < 0 || bytes.Compare(kbz, end) >= 0 {
			// we don't want to break out of the iteration since cache isn't sorted
			return true
		}
		cv := value.(*cValue)
		if cv.value == nil {
			delete(keyStrs, key.(string))
		} else {
			keyStrs[key.(string)] = struct{}{}
		}
		return true
	})
	for k := range keyStrs {
		res = append(res, k)
	}
	return res
}
