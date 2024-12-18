package nsblockstore

import (
	"cosmossdk.io/log"
	"fmt"
	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/types"
	"github.com/near/borsh-go"
	"strconv"
	"sync"
)

// NSBlockStore defines the interface used by the ConsensusState.
type NSBlockStore interface {
	Base() int64
	Height() int64
	Size() int64

	LoadBlock(height int64) *NamespaceBLock

	CacheBlock(block *NamespaceBLock)

	CurrentBlock() *NamespaceBLock

	UpdateBlock(block *NamespaceBLock)

	Commit()

	SaveBlock(height int64, hash []byte, block *NamespaceBLock)

	LoadBlockByHash(hash []byte) *NamespaceBLock

	LoadBlockByRelatedHeight(relatedHeight int64) *NamespaceBLock

	SetNamespaceStateActive()

	GetNamespaceState() bool

	LoadBlockCommit(height int64) *types.Commit
	LoadSeenCommit(height int64) *types.Commit
	LoadBlockExtendedCommit(height int64) *types.ExtendedCommit

	DeleteLatestBlock() error

	Close() error
}

type BlockStoreState struct {
	Base   int64
	Height int64
}

type NamespaceBLockStore struct {
	db dbm.DB

	logger log.Logger
	// mtx guards access to the struct fields listed below it. Although we rely on the database
	// to enforce fine-grained concurrency control for its data, we need to make sure that
	// no external observer can get data from the database that is not in sync with the fields below,
	// and vice-versa. Hence, when updating the fields below, we use the mutex to make sure
	// that the database is also up to date. This prevents any concurrent external access from
	// obtaining inconsistent data.
	// The only reason for keeping these fields in the struct is that the data
	// can't efficiently be queried from the database since the key encoding we use is not
	// lexicographically ordered (see https://github.com/tendermint/tendermint/issues/4567).
	//mtx    cmtsync.RWMutex
	base   int64
	height int64

	namespaceId uint64
	cancheBlock *NamespaceBLock
	sync.RWMutex
}

const baseBlockStoreStateKey = "base_block_state"

// blockStoreState key-value, represent whether the namespace block store is active
const blockStoreActiveStateKey = "block_store_active_state"

func calcBlockDataKey(height int64) []byte {
	return []byte(fmt.Sprintf("BD:%v", height))
}

func calcBlockHashKey(Hash []byte) []byte {
	return []byte(fmt.Sprintf("H:%v", Hash))
}

func calcBaseBlockStoreSateKey() []byte {
	return []byte(baseBlockStoreStateKey)
}

func calcNamespaceStateKey() []byte {
	return []byte(fmt.Sprintf("NSAS:%v", blockStoreActiveStateKey))
}

func calcBFTHeightKey(height int64) []byte {
	return []byte(fmt.Sprintf("BFTH:%v", height))
}

func NewNamespaceBlockStore(db dbm.DB, namespaceId uint64, logger log.Logger) *NamespaceBLockStore {
	storeState := loadStoreBaseState(db)
	return &NamespaceBLockStore{
		db:          db,
		base:        storeState.Base,
		height:      storeState.Height,
		namespaceId: namespaceId,
		logger:      logger.With("module", "nsblockstore"),
	}
}

type NamespaceBLock struct {
	// namespace block data, include header and body
	Data []byte

	// RelatedCometBFTBlockHeight, relation with cometBFT block height, that can make NamespaceBLockStore
	// not save txs data, through cometBFT block height, get cometBFT block, then fill with namespace txs data
	RelatedCometBFTBlockHeight int64

	// the Data block hash
	Hash []byte

	// the data block heights
	Height int64
}

func (n *NamespaceBLockStore) Base() int64 {
	n.RLock()
	defer n.RUnlock()
	return n.base
}

func (n *NamespaceBLockStore) Height() int64 {
	n.RLock()
	defer n.RUnlock()
	return n.height
}

func (n *NamespaceBLockStore) Size() int64 {
	n.RLock()
	defer n.RUnlock()
	if n.height == 0 {
		return 0
	}
	return n.height - n.base + 1
}

func (n *NamespaceBLockStore) LoadBlock(height int64) *NamespaceBLock {
	n.RLock()
	blockData, err := n.db.Get(calcBlockDataKey(height))
	n.RUnlock()
	if err != nil {
		panic(err)
	}
	if len(blockData) == 0 {
		return nil
	}
	var namespaceBlock NamespaceBLock
	err = borsh.Deserialize(&namespaceBlock, blockData)
	if err != nil {
		panic(fmt.Sprintf("Error reading namespace %v block: %v", n.namespaceId, err))
	}
	return &namespaceBlock
}

func (n *NamespaceBLockStore) SaveBlock(height int64, hash []byte, block *NamespaceBLock) {
	blockData, err := borsh.Serialize(*block)
	if err != nil {
		panic(err)
	}
	if height != n.height+1 && n.height != n.base {
		oldBlock := n.LoadBlock(height)
		// cosmos block Rollback or replay
		if oldBlock.RelatedCometBFTBlockHeight != block.RelatedCometBFTBlockHeight {
			panic(fmt.Errorf("BlockStore can only save contiguous blocks. Wanted %v, got %v", n.height+1, height))
		}
		n.height = height - 1
	}
	batch := n.db.NewBatch()
	defer batch.Close()

	err = batch.Set(calcBlockDataKey(height), blockData)
	if err != nil {
		panic(err)
	}
	err = batch.Set(calcBlockHashKey(hash), []byte(fmt.Sprintf("%d", height)))
	if err != nil {
		panic(err)
	}

	err = batch.Set(calcBFTHeightKey(block.RelatedCometBFTBlockHeight), []byte(fmt.Sprintf("%d", height)))
	if err != nil {
		panic(err)
	}

	n.Lock()
	defer n.Unlock()
	if height != n.base {
		n.height += 1
	}

	storeState := BlockStoreState{
		Base:   n.base,
		Height: n.height,
	}
	stateData, err := borsh.Serialize(storeState)
	if err != nil {
		panic(err)
	}

	err = batch.Set(calcBaseBlockStoreSateKey(), stateData)
	if err != nil {
		panic(err)
	}
	err = batch.Write()
	if err != nil {
		panic(err)
	}
	n.logger.Info("save namespace Block", "height", block.Height, "cosmos height", block.RelatedCometBFTBlockHeight, "current hash", fmt.Sprintf("%X", block.Hash))
}

func (n *NamespaceBLockStore) CacheBlock(block *NamespaceBLock) {
	if n.cancheBlock != nil {
		panic("Cache block not commit in cache block state")
	}
	n.cancheBlock = block
}

func (n *NamespaceBLockStore) CurrentBlock() *NamespaceBLock {
	if n.cancheBlock != nil {
		return n.cancheBlock
	}
	return n.LoadBlock(n.height)
}

func (n *NamespaceBLockStore) UpdateBlock(block *NamespaceBLock) {
	if n.cancheBlock == nil {
		panic("Cache block not generate before")
	}
	n.cancheBlock = block
}

func (n *NamespaceBLockStore) Commit() {
	if n.cancheBlock == nil {
		return
	}
	cacheBlock := n.cancheBlock
	n.SaveBlock(cacheBlock.Height, cacheBlock.Hash, cacheBlock)
	n.cancheBlock = nil
}

func (n *NamespaceBLockStore) LoadBlockByHash(hash []byte) *NamespaceBLock {
	n.RLock()
	defer n.RUnlock()
	heightData, err := n.db.Get(calcBlockHashKey(hash))
	if err != nil {
		panic(err)
	}
	if heightData == nil {
		return nil
	}
	height, err := strconv.Atoi(string(heightData))
	if err != nil {
		panic(fmt.Sprintf("Error reading block by hash %v", err))
	}
	return n.LoadBlock(int64(height))
}

func (n *NamespaceBLockStore) LoadBlockByRelatedHeight(relatedHeight int64) *NamespaceBLock {
	heightData, err := n.db.Get(calcBFTHeightKey(relatedHeight))
	if err != nil {
		panic(err)
	}
	if heightData == nil {
		return nil
	}
	height, err := strconv.Atoi(string(heightData))
	if err != nil {
		panic(fmt.Sprintf("Error reading block by hash %v", err))
	}
	n.RLock()
	defer n.RUnlock()
	return n.LoadBlock(int64(height))
}

func (n *NamespaceBLockStore) LoadBlockCommit(height int64) *types.Commit {
	//TODO implement me
	panic("implement me")
}

func (n *NamespaceBLockStore) LoadSeenCommit(height int64) *types.Commit {
	//TODO implement me
	panic("implement me")
}

func (n *NamespaceBLockStore) LoadBlockExtendedCommit(height int64) *types.ExtendedCommit {
	//TODO implement me
	panic("implement me")
}

func (n *NamespaceBLockStore) DeleteLatestBlock() error {
	//TODO implement me
	panic("implement me")
}

func (n *NamespaceBLockStore) Close() error {
	return n.db.Close()
}

func (n *NamespaceBLockStore) SetNamespaceStateActive() {
	n.Lock()
	n.Unlock()
	n.db.Set(calcNamespaceStateKey(), []byte{1})
}

func (n *NamespaceBLockStore) GetNamespaceState() bool {
	n.RLock()
	defer n.RUnlock()
	val, err := n.db.Get(calcNamespaceStateKey())
	if err != nil {
		panic("Read data from db failed")
	}
	if val == nil {
		return false
	}
	return val[0] == 1
}

func loadStoreBaseState(db dbm.DB) BlockStoreState {
	stateData, err := db.Get(calcBaseBlockStoreSateKey())
	if err != nil {
		panic("read namespace block store failed")
	}
	var ret BlockStoreState
	if stateData != nil {
		borsh.Deserialize(&ret, stateData)
	}
	return ret
}
