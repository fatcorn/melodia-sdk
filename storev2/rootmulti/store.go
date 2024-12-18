package rootmulti

import (
	"cosmossdk.io/errors"
	"cosmossdk.io/store/cache"
	"cosmossdk.io/store/cachemulti"
	"cosmossdk.io/store/iavl"
	"cosmossdk.io/store/mem"
	cosmosIavl "github.com/cosmos/iavl"
	iavlProto "github.com/cosmos/iavl/proto"

	storemetrics "cosmossdk.io/store/metrics"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/rootmulti"
	snapshot "cosmossdk.io/store/snapshots/types"
	"cosmossdk.io/store/transient"
	"cosmossdk.io/store/types"
	storetypes "cosmossdk.io/store/types"
	"encoding/json"
	goerrors "errors"

	"fmt"
	hashmetrics "github.com/hashicorp/go-metrics"

	cmtdbm "github.com/cometbft/cometbft-db"
	abci "github.com/cometbft/cometbft/abci/types"
	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	bfttypes "github.com/cometbft/cometbft/types"
	db "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/nsblockstore"
	snapshottypes "github.com/cosmos/cosmos-sdk/snapshots/types"
	"github.com/cosmos/cosmos-sdk/storev2/commitment"
	"github.com/cosmos/cosmos-sdk/storev2/state"
	"github.com/cosmos/cosmos-sdk/telemetry"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	io2 "github.com/cosmos/gogoproto/io"
	protoio "github.com/gogo/protobuf/io"
	commonerrors "github.com/sei-protocol/sei-db/common/errors"
	"github.com/sei-protocol/sei-db/config"
	"github.com/sei-protocol/sei-db/proto"
	"github.com/sei-protocol/sei-db/sc"
	sctypes "github.com/sei-protocol/sei-db/sc/types"
	"github.com/sei-protocol/sei-db/ss"
	"github.com/sei-protocol/sei-db/ss/pruning"
	sstypes "github.com/sei-protocol/sei-db/ss/types"
	dbm "github.com/tendermint/tm-db"

	"golang.org/x/exp/slices"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	_ types.CommitMultiStore = (*Store)(nil)
	_ types.Queryable        = (*Store)(nil)
)

type Store struct {
	logger          log.Logger
	mtx             sync.RWMutex
	scStore         sctypes.Committer
	ssStore         sstypes.StateStore
	lastCommitInfo  *types.CommitInfo
	storesParams    map[types.StoreKey]storeParams
	storeKeys       map[string]types.StoreKey
	ckvStores       map[types.StoreKey]types.CommitKVStore
	pendingChanges  chan VersionedChangesets
	pruningManager  *pruning.Manager
	commitHeader    cmtproto.Header
	removalMap      map[types.StoreKey]bool
	interBlockCache types.MultiStorePersistentCache
}

func (rs *Store) Commit() types.CommitID {
	return rs.Commit1(true)
}

func (rs *Store) WorkingHash() []byte {
	hash, err := rs.GetWorkingHash()
	if err != nil {
		//TODO implement me
		panic("store get wrking hash error")
	}
	return hash
}

func (rs *Store) SetPruning(options pruningtypes.PruningOptions) {
	//TODO implement me
	//panic("implement me")
}

func (rs *Store) GetPruning() pruningtypes.PruningOptions {
	return pruningtypes.NewPruningOptions(pruningtypes.PruningDefault)
}

func (rs *Store) ClearCache() {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) CacheWrap() types.CacheWrap {
	return rs.CacheWrap()
}

func (rs *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) LatestVersion() int64 {
	latestVersion := rs.lastCommitInfo.Version
	//if rs.ssStore != nil {
	//	version, err := rs.ssStore.GetEarliestVersion()
	//	if err != nil {
	//		return latestVersion
	//	}
	//	return version
	//}
	return latestVersion
}

func (rs *Store) Snapshot(height uint64, protoWriter io2.Writer) error {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) Restore(height uint64, format uint32, protoReader io2.Reader) (snapshot.SnapshotItem, error) {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) NamespaceWorkingHash(moudleNames []string, namespaceId uint64) []byte {
	storeInfos := make([]types.StoreInfo, 0, len(rs.ckvStores))
	//storeKeys := keysFromStoreKeyMap(rs.stores)

	//namespaceIdStr :=
	storeKeys := make([]types.StoreKey, 0)

	for _, name := range moudleNames {
		storeKeyName := name + "_" + strconv.FormatUint(namespaceId, 10)
		storeKey := rs.storeKeys[storeKeyName]
		storeKeys = append(storeKeys, storeKey)
	}

	for _, key := range storeKeys {
		store := rs.ckvStores[key]

		if store.GetStoreType() != types.StoreTypeIAVL {
			continue
		}

		if !rs.removalMap[key] {
			si := types.StoreInfo{
				Name: key.Name(),
				CommitId: types.CommitID{
					Hash: store.WorkingHash(),
				},
			}
			storeInfos = append(storeInfos, si)
		}
	}

	sort.SliceStable(storeInfos, func(i, j int) bool {
		return storeInfos[i].Name < storeInfos[j].Name
	})

	return types.CommitInfo{StoreInfos: storeInfos}.Hash()
}

// SetCommitHeader sets the commit block header of the store.
func (rs *Store) SetCommitHeader(h cmtproto.Header) {
	rs.commitHeader = h
}

func (rs *Store) MountStoreWithDB(key types.StoreKey, typ types.StoreType, db db.DB) {
	if key == nil {
		panic("MountIAVLStore() key cannot be nil")
	}
	if _, ok := rs.storesParams[key]; ok {
		panic(fmt.Sprintf("store duplicate store key %v", key))
	}
	if _, ok := rs.storeKeys[key.Name()]; ok {
		panic(fmt.Sprintf("store duplicate store key name %v", key))
	}
	rs.storesParams[key] = newStoreParams(key, typ)
	rs.storeKeys[key.Name()] = key
}

func (rs *Store) AddListeners(keys []types.StoreKey) {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) PopStateCache() []*types.StoreKVPair {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) SetMetrics(metrics storemetrics.StoreMetrics) {
	//TODO implement me
	panic("implement me")
}

func (rs *Store) DynamicAddNamesapceStore(newKeys ...types.StoreKey) error {
	//infos := make(map[string]types.StoreInfo)

	rs.logger.Info("AddNamesapceStore", "add new namesapce store", newKeys)
	//cInfo := &types.CommitInfo{}
	//
	//// load each Store (note this doesn't panic on unmounted keys now)
	//newStores := make(map[types.StoreKey]types.CommitKVStore)

	//storesKeys := make([]types.StoreKey, 0, len(rs.storesParams))
	validKeys := make([]types.StoreKey, 0)
	for _, key := range newKeys {
		// in same namespace with multi vm moudle may lead stores
		if _, ok := rs.storeKeys[key.Name()]; ok {
			continue
		}
		validKeys = append(validKeys, key)
		var storeType types.StoreType
		switch key.(type) {
		case *types.KVStoreKey:
			storeType = types.StoreTypeIAVL
		case *types.TransientStoreKey:
			storeType = types.StoreTypeTransient
		default:
			panic("Only 2 types store key was rejected for now")
		}
		rs.MountStoreWithDB(key, storeType, nil)
	}

	for _, key := range validKeys {
		storeParams := rs.storesParams[key]
		//storeParams.initialVersion = uint64(rs.commitHeader.Height + 1)
		//commitID.Version = rs.commitHeader.Height

		//// If it has been added, set the initial version
		//if upgrades.IsAdded(key.Name()) || upgrades.RenamedFrom(key.Name()) != "" {
		//	storeParams.initialVersion = uint64(ver) + 1
		//} else if commitID.Version != ver && storeParams.typ == types.StoreTypeIAVL {
		//	return fmt.Errorf("version of store %s mismatch root store's version; expected %d got %d; new stores should be added using StoreUpgrades", key.Name(), ver, commitID.Version)
		//}
		//storeParams.initialVersion = uint64(rs.commitHeader.Height)

		store, err := rs.loadCommitStoreFromParams(key, storeParams)
		if err != nil {
			return goerrors.New(fmt.Sprintf("Load Namespace store %s failed cause %v", key.Name(), err))
		}
		// todo, use appointed version to create store, rather than for commit

		if commitKVStoreCache, ok := store.(*cache.CommitKVStoreCache); ok {
			if ivalStore, ok := commitKVStoreCache.CommitKVStore.(*iavl.Store); ok {
				ivalStore.SetInitialVersion(rs.commitHeader.Height)
			}
		}

		if commitKVStoreCache, ok := store.(*cache.CommitKVStoreBigCache); ok {
			if ivalStore, ok := commitKVStoreCache.CommitKVStore.(*iavl.Store); ok {
				ivalStore.SetInitialVersion(rs.commitHeader.Height)
			}
		}
		if store.GetStoreType() != types.StoreTypeIAVL {
			_ = store.Commit()
		}

		//for i := 0; i < int(rs.commitHeader.Height); i++ {
		//	store.Commit()
		//}

		rs.ckvStores[key] = store
		rs.storeKeys[key.Name()] = key
	}

	return nil
}

func (rs *Store) getCommitID(infos map[string]types.StoreInfo, name string) types.CommitID {
	info, ok := infos[name]
	if !ok {
		return types.CommitID{}
	}

	return info.CommitId
}

type VersionedChangesets struct {
	Version    int64
	Changesets []*proto.NamedChangeSet
}

func NewStore(
	homeDir string,
	logger log.Logger,
	scConfig config.StateCommitConfig,
	ssConfig config.StateStoreConfig,
) *Store {
	scStore := sc.NewCommitStore(homeDir, logger, scConfig)
	store := &Store{
		logger:         logger,
		scStore:        scStore,
		storesParams:   make(map[types.StoreKey]storeParams),
		storeKeys:      make(map[string]types.StoreKey),
		ckvStores:      make(map[types.StoreKey]types.CommitKVStore),
		pendingChanges: make(chan VersionedChangesets, 1000),
		removalMap:     make(map[types.StoreKey]bool),
	}
	if ssConfig.Enable {
		ssStore, err := ss.NewStateStore(logger, homeDir, ssConfig)
		if err != nil {
			panic(err)
		}
		// Check whether SC was enabled before but SS was not
		ssVersion, _ := ssStore.GetLatestVersion()
		scVersion, _ := scStore.GetLatestVersion()
		if ssVersion <= 0 && scVersion > 0 {
			panic("Enabling SS store without state sync could cause data corruption")
		}
		if err = ss.RecoverStateStore(logger, homeDir, ssStore); err != nil {
			panic(err)
		}
		store.ssStore = ssStore
		go store.StateStoreCommit()
	}
	return store

}

// Commit implements interface Committer, called by ABCI Commit
func (rs *Store) Commit1(bumpVersion bool) types.CommitID {
	if !bumpVersion {
		panic("Commit should always bump version in root multistore")
	}
	commitStartTime := time.Now()
	defer telemetry.MeasureSince(commitStartTime, "storeV2", "sc", "commit", "latency")
	if err := rs.flush(); err != nil {
		panic(err)
	}

	rs.mtx.Lock()
	defer rs.mtx.Unlock()
	for _, store := range rs.ckvStores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			_ = store.Commit()
		}
	}
	// Commit to SC Store
	_, err := rs.scStore.Commit()
	if err != nil {
		panic(err)
	}

	// reset the removalMap
	rs.removalMap = make(map[types.StoreKey]bool)

	// The underlying sc store might be reloaded, reload the store as well.
	for key := range rs.ckvStores {
		store := rs.ckvStores[key]
		if store.GetStoreType() == types.StoreTypeIAVL {
			rs.ckvStores[key], err = rs.loadCommitStoreFromParams(key, rs.storesParams[key])
			if err != nil {
				panic(fmt.Errorf("inconsistent store map, store %s not found", key.Name()))
			}
		}
	}

	rs.lastCommitInfo = convertCommitInfo(rs.scStore.LastCommitInfo())
	rs.lastCommitInfo = amendCommitInfo(rs.lastCommitInfo, rs.storesParams)
	return rs.lastCommitInfo.CommitID()
}

// StateStoreCommit is a background routine to apply changes to SS store
func (rs *Store) StateStoreCommit() {
	for pendingChangeSet := range rs.pendingChanges {
		version := pendingChangeSet.Version
		telemetry.SetGauge(float32(version), "storeV2", "ss", "version")
		for _, cs := range pendingChangeSet.Changesets {
			if err := rs.ssStore.ApplyChangeset(version, cs); err != nil {
				panic(err)
			}

		}
	}
}

// Flush all the pending changesets to commit store.
func (rs *Store) flush() error {
	t1 := time.Now()
	var changeSets []*proto.NamedChangeSet
	currentVersion := rs.lastCommitInfo.Version + 1
	for key := range rs.ckvStores {
		// it'll unwrap the inter-block cache
		store := rs.GetCommitKVStore(key)
		if store.GetParentStore() != nil {
			if commitStore, ok := store.GetParentStore().(*commitment.Store); ok {
				cs := commitStore.PopChangeSet()
				if len(cs.Pairs) > 0 {
					changeSets = append(changeSets, &proto.NamedChangeSet{
						Name:      key.Name(),
						Changeset: cs,
					})
				}
			}
		} else {
			if commitStore, ok := store.(*commitment.Store); ok {
				cs := commitStore.PopChangeSet()
				if len(cs.Pairs) > 0 {
					changeSets = append(changeSets, &proto.NamedChangeSet{
						Name:      key.Name(),
						Changeset: cs,
					})
				}
			}
		}
	}
	t2 := time.Now()

	if changeSets != nil && len(changeSets) > 0 {
		sort.SliceStable(changeSets, func(i, j int) bool {
			return changeSets[i].Name < changeSets[j].Name
		})
		if rs.ssStore != nil {
			var copiedChangeSets []*proto.NamedChangeSet
			for _, cs := range changeSets {
				var pairs []*iavlProto.KVPair
				for _, ss := range cs.Changeset.Pairs {
					pair := iavlProto.KVPair{
						Key:    ss.Key,
						Value:  ss.Value,
						Delete: ss.Delete,
					}
					pairs = append(pairs, &pair)
				}
				set := proto.NamedChangeSet{
					Name: cs.Name,
					Changeset: cosmosIavl.ChangeSet{
						Pairs: pairs,
					},
				}
				copiedChangeSets = append(copiedChangeSets, &set)
			}
			rs.pendingChanges <- VersionedChangesets{
				Version:    currentVersion,
				Changesets: copiedChangeSets,
			}
		}
	}
	t3 := time.Now()

	err := rs.scStore.ApplyChangeSets(changeSets)
	t4 := time.Now()
	println("flush time=======", "set changs set ", t2.Sub(t1).String(), "sort set ", t3.Sub(t2).String(), "apply change set", t4.Sub(t3).String(), "version ", currentVersion, "size", len(changeSets))
	return err
}

func (rs *Store) Close() error {
	err := rs.scStore.Close()
	close(rs.pendingChanges)
	if rs.ssStore != nil {
		err = commonerrors.Join(err, rs.ssStore.Close())
	}
	return err
}

// LastCommitID Implements interface Committer
func (rs *Store) LastCommitID() types.CommitID {
	if rs.lastCommitInfo == nil {
		v, err := rs.scStore.GetLatestVersion()
		if err != nil {
			panic(fmt.Errorf("failed to get latest version: %w", err))
		}
		return types.CommitID{Version: v}
	}

	return rs.lastCommitInfo.CommitID()
}

// NewNamespaceDB handle internal Event to execute related logic
func CreateNamespaceDB(namespaceId uint64, moudleName string, dbRootDir string) (cmtdbm.DB, error) {
	namespaceDB, err := cmtcfg.DefaultDBProvider(&cmtcfg.DBContext{
		ID:     "namespace" + "_" + strconv.FormatUint(namespaceId, 10) + "_" + moudleName,
		Config: cmtcfg.DefaultConfig().SetRoot(dbRootDir).AddDBPath("namespace").SetBanCacheDB(true).SetDBBackend("goleveldb"),
	})
	return namespaceDB, err
}

func (rs *Store) LoadNamespaceStoreInfo(home string) (*nsblockstore.NamespaceStoreDBInfo, error) {
	validNamespaces := rs.ListDBNamespace()
	tree := rs.scStore.GetTreeByName("chainservice_0")

	namespace := rs.ListTreeNamespace(tree)
	validNamespaces = append(validNamespaces, namespace...)

	namespacesDBMap := make(map[string]cmtdbm.DB)
	namespacesModuleName := make(map[string]nsblockstore.NamespaceModule)
	moduleRelatedModules := make(map[string][]string)
	for _, namespace := range validNamespaces {
		namespaceDB, err := CreateNamespaceDB(uint64(namespace.NamespaceId), namespace.Module, home)
		if err != nil {
			return nil, err
		}
		blockStoreKey := nsblockstore.CaculateStoreKey(uint64(namespace.NamespaceId), namespace.Module)
		namespacesDBMap[blockStoreKey] = namespaceDB
		namespacesModuleName[blockStoreKey] = nsblockstore.NamespaceModule{
			NamespaceId: uint64(namespace.NamespaceId),
			ModuleName:  namespace.Module,
		}
		moduleRelatedModules[namespace.Module] = namespace.ListAllDependentMoudlesName()
	}

	namespaceDBInfo := nsblockstore.NamespaceStoreDBInfo{
		NamespaceStoreDBs:    namespacesDBMap,
		DbRootDir:            home,
		NamespacesModuleName: namespacesModuleName,
		ModuleRelatedModules: moduleRelatedModules,
	}
	return &namespaceDBInfo, nil
}

// Implements interface Store
func (rs *Store) GetStoreType() types.StoreType {
	return types.StoreTypeMulti
}

//func (rs *Store) CacheWrapWithListeners(k types.StoreKey, listeners []types.WriteListener) types.CacheWrap {
//	return rs.CacheMultiStore().CacheWrapWithListeners(k, listeners)
//}

// Implements interface MultiStore
func (rs *Store) CacheMultiStore() types.CacheMultiStore {
	rs.mtx.RLock()
	defer rs.mtx.RUnlock()
	stores := make(map[types.StoreKey]types.CacheWrapper)
	for k, v := range rs.ckvStores {
		store := types.KVStore(v)
		stores[k] = store
	}
	return cachemulti.NewStore(nil, stores, rs.storeKeys, nil, nil)
}

// CacheMultiStoreWithVersion Implements interface MultiStore
// used to createQueryContext, abci_query or grpc query service.
func (rs *Store) CacheMultiStoreWithVersion(version int64) (types.CacheMultiStore, error) {
	if version <= 0 || (rs.lastCommitInfo != nil && version == rs.lastCommitInfo.Version) {
		return rs.CacheMultiStore(), nil
	}
	rs.mtx.RLock()
	defer rs.mtx.RUnlock()
	stores := make(map[types.StoreKey]types.CacheWrapper)
	// add the transient/mem stores registered in current app.
	for k, store := range rs.ckvStores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			stores[k] = store
		}
	}
	// TODO: May need to add historical SC store as well for nodes that doesn't enable ss but still need historical queries

	// add SS stores for historical queries
	if rs.ssStore != nil {
		for k, store := range rs.ckvStores {
			if store.GetStoreType() == types.StoreTypeIAVL {
				stores[k] = state.NewStore(rs.ssStore, k, version)
			}
		}
	}

	return cachemulti.NewStore(nil, stores, rs.storeKeys, nil, nil), nil
}

func (rs *Store) CacheMultiStoreForExport(version int64) (types.CacheMultiStore, error) {
	if version <= 0 || (rs.lastCommitInfo != nil && version == rs.lastCommitInfo.Version) {
		return rs.CacheMultiStore(), nil
	}
	rs.mtx.RLock()
	defer rs.mtx.RUnlock()
	stores := make(map[types.StoreKey]types.CacheWrapper)
	// add the transient/mem stores registered in current app.
	for k, store := range rs.ckvStores {
		if store.GetStoreType() != types.StoreTypeIAVL {
			stores[k] = store
		}
	}
	// add SC stores for historical queries
	scStore, err := rs.scStore.LoadVersion(version, true)
	if err != nil {
		return nil, err
	}
	for k, store := range rs.ckvStores {
		if store.GetStoreType() == types.StoreTypeIAVL {
			tree := scStore.GetTreeByName(k.Name())
			stores[k] = commitment.NewStore(tree, rs.logger)
		}
	}
	cacheMs := cachemulti.NewStore(nil, stores, rs.storeKeys, nil, nil)
	// We need this because we need to make sure sc is closed after being used to release the resources
	cacheMs.AddCloser(scStore)
	return cacheMs, nil
}

// GetStore Implements interface MultiStore
func (rs *Store) GetStore(key types.StoreKey) types.Store {
	return rs.ckvStores[key]
}

// GetKVStore Implements interface MultiStore
func (rs *Store) GetKVStore(key types.StoreKey) types.KVStore {
	validKey := rs.storeKeys[key.Name()]

	return rs.ckvStores[validKey]
}

// Implements interface MultiStore
func (rs *Store) TracingEnabled() bool {
	return false
}

// Implements interface MultiStore
func (rs *Store) SetTracer(_ io.Writer) types.MultiStore {
	return nil
}

// Implements interface MultiStore
func (rs *Store) SetTracingContext(types.TraceContext) types.MultiStore {
	return nil
}

// Implements interface Snapshotter
// not needed, memiavl manage its own snapshot/pruning strategy
func (rs *Store) PruneSnapshotHeight(_ int64) {
}

// Implements interface Snapshotter
// not needed, memiavl manage its own snapshot/pruning strategy
func (rs *Store) SetSnapshotInterval(_ uint64) {
}

// Implements interface CommitMultiStore
func (rs *Store) MountStoreWithDB1(key types.StoreKey, typ types.StoreType, _ dbm.DB) {
	if key == nil {
		panic("MountIAVLStore() key cannot be nil")
	}
	if _, ok := rs.storesParams[key]; ok {
		panic(fmt.Sprintf("store duplicate store key %v", key))
	}
	if _, ok := rs.storeKeys[key.Name()]; ok {
		panic(fmt.Sprintf("store duplicate store key name %v", key))
	}
	rs.storesParams[key] = newStoreParams(key, typ)
	rs.storeKeys[key.Name()] = key
}

// Implements interface CommitMultiStore
func (rs *Store) GetCommitStore(key types.StoreKey) types.CommitStore {
	return rs.GetCommitKVStore(key)
}

// GetCommitKVStore Implements interface CommitMultiStore
func (rs *Store) GetCommitKVStore(key types.StoreKey) types.CommitKVStore {
	return rs.ckvStores[key]
}

// Implements interface CommitMultiStore
// used by normal node startup.
func (rs *Store) LoadLatestVersion() error {
	return rs.LoadVersionAndUpgrade(0, nil)
}

// Implements interface CommitMultiStore
func (rs *Store) LoadLatestVersionAndUpgrade(upgrades *types.StoreUpgrades) error {
	return rs.LoadVersionAndUpgrade(0, upgrades)
}

// ListDBNamespace list registered namespace in chainservice
func (rs *Store) ListDBNamespace() []*bfttypes.NamespacePort {
	var nsPorts []*bfttypes.NamespacePort
	latestVersion, err := rs.ssStore.GetLatestVersion()
	if err != nil {
		return nil
	}

	namesapceIdPrefixKey := []byte("s/k:chainservice_0/fnamespace_store")

	iter, _ := rs.ssStore.Iterator("chainservice_0", latestVersion, namesapceIdPrefixKey, storetypes.PrefixEndBytes(namesapceIdPrefixKey))
	for iter.Valid() {
		value := iter.Value()

		validBytesStartIndex := slices.IndexFunc(value, func(val byte) bool {
			if val == []byte("{")[0] {
				return true
			}
			return false
		})
		var nsPort bfttypes.NamespacePort
		err := json.Unmarshal(value[validBytesStartIndex:], &nsPort)
		if err != nil {
			fmt.Printf("Unmarshal namespacePort failed %v\n", err)
		} else {
			nsPorts = append(nsPorts, &nsPort)
		}
		iter.Next()
	}
	var induplicateNamespace []*bfttypes.NamespacePort
	uniqueNamespaces := make(map[string]bool)
	for _, port := range nsPorts {
		namespaceModuleKey := nsblockstore.CaculateStoreKey(uint64(port.NamespaceId), port.Module)
		if ok := uniqueNamespaces[namespaceModuleKey]; ok {
			continue
		}
		uniqueNamespaces[namespaceModuleKey] = true
		induplicateNamespace = append(induplicateNamespace, port)
	}

	return induplicateNamespace
}

// ListDBNamespace list registered namespace in chainservice
func (rs *Store) ListTreeNamespace(name sctypes.Tree) []*bfttypes.NamespacePort {

	namesapceIdPrefixKey := []byte("namespace_store")
	var nsPorts []*bfttypes.NamespacePort

	iter := name.Iterator(namesapceIdPrefixKey, storetypes.PrefixEndBytes(namesapceIdPrefixKey), true)
	for iter.Valid() {
		value := iter.Value()

		validBytesStartIndex := slices.IndexFunc(value, func(val byte) bool {
			if val == []byte("{")[0] {
				return true
			}
			return false
		})
		var nsPort bfttypes.NamespacePort
		err := json.Unmarshal(value[validBytesStartIndex:], &nsPort)
		if err != nil {
			fmt.Printf("Unmarshal namespacePort failed %v\n", err)
		} else {
			nsPorts = append(nsPorts, &nsPort)
		}
		iter.Next()
	}

	var induplicateNamespace []*bfttypes.NamespacePort
	uniqueNamespaces := make(map[string]bool)
	for _, port := range nsPorts {
		namespaceModuleKey := nsblockstore.CaculateStoreKey(uint64(port.NamespaceId), port.Module)
		if ok := uniqueNamespaces[namespaceModuleKey]; ok {
			continue
		}
		uniqueNamespaces[namespaceModuleKey] = true
		induplicateNamespace = append(induplicateNamespace, port)
	}

	return induplicateNamespace
}

func (rs *Store) LoadValidNamespaceStore(tree sctypes.Tree, keys ...storetypes.StoreKey) []storetypes.StoreKey {

	var namespaceStores []storetypes.StoreKey
	var nsPorts []*bfttypes.NamespacePort
	if tree == nil {
		nsPorts = rs.ListDBNamespace()
	} else {
		nsPorts = rs.ListTreeNamespace(tree)
	}
	namespaceStores = NamespaceStoresFromNSPort(nsPorts, nil)

	return namespaceStores
}

// NamespaceStoresFromNSPort create namesapce store key
func NamespaceStoresFromNSPort(indunplicateNamespace []*bfttypes.NamespacePort, manager *nsblockstore.NamespaceStoreManager) []storetypes.StoreKey {
	namespaceStores := make([]storetypes.StoreKey, 0)
	// for avoiding namespaceStores repeat mount
	namespaceStoresMap := make(map[string]bool)
	for _, namesapceInfo := range indunplicateNamespace {
		vmMoudleStoreKey := storetypes.NewKVStoreKey(namesapceInfo.Module + "_" + strconv.FormatUint(uint64(namesapceInfo.NamespaceId), 10))
		if _, ok := namespaceStoresMap[vmMoudleStoreKey.Name()]; !ok {
			namespaceStores = append(namespaceStores, vmMoudleStoreKey)
			namespaceStoresMap[vmMoudleStoreKey.Name()] = true
		}

		for _, info := range namesapceInfo.VMModuleDependentModules {
			keyName := info.ModuleName + "_" + strconv.FormatUint(uint64(namesapceInfo.NamespaceId), 10)
			dependentModuleStoreKey := storetypes.NewStoreKeyByDMStoreInfo(keyName, bfttypes.DependModuleStoreType(info.StoreType))
			if _, ok := namespaceStoresMap[dependentModuleStoreKey.Name()]; !ok {
				namespaceStores = append(namespaceStores, dependentModuleStoreKey)
				namespaceStoresMap[dependentModuleStoreKey.Name()] = true
			}
		}
	}
	return namespaceStores
}

// MountStore mounts a store to the provided key in the BaseApp multistore,
// using the default DB.
func (rs *Store) MountStore(tree sctypes.Tree, version int64) ([]types.StoreKey, error) {
	namespace := rs.LoadValidNamespaceStore(tree)

	for _, key := range namespace {
		if _, ok := rs.storesParams[key]; ok {
			continue
		}
		if _, ok := rs.storeKeys[key.Name()]; ok {
			continue
		}
		switch key.(type) {
		case *storetypes.KVStoreKey:
			rs.MountStoreWithDB(key, storetypes.StoreTypeIAVL, nil)
		case *storetypes.TransientStoreKey:
			rs.MountStoreWithDB(key, storetypes.StoreTypeTransient, nil)

		case *storetypes.MemoryStoreKey:
			rs.MountStoreWithDB(key, storetypes.StoreTypeMemory, nil)
		default:
			panic(fmt.Sprintf("Unrecognized store key type :%T", key))
		}
	}
	if tree != nil && len(namespace) > 0 {
		storesKeys := make([]types.StoreKey, 0, len(rs.storesParams))
		for key := range rs.storesParams {
			storesKeys = append(storesKeys, key)
		}
		// deterministic iteration order for upgrades
		sort.Slice(storesKeys, func(i, j int) bool {
			return storesKeys[i].Name() < storesKeys[j].Name()
		})

		initialStores := make([]string, 0, len(storesKeys))
		for _, key := range storesKeys {
			if rs.storesParams[key].typ == types.StoreTypeIAVL {
				initialStores = append(initialStores, key.Name())
			}
		}
		rs.scStore.Initialize(initialStores)
		if _, err := rs.scStore.LoadVersion(version, false); err != nil {
			return nil, nil
		}
		return storesKeys, nil
	}
	return nil, nil
}

// Implements interface CommitMultiStore
// used by node startup with UpgradeStoreLoader
func (rs *Store) LoadVersionAndUpgrade(version int64, upgrades *types.StoreUpgrades) error {

	_, err := rs.MountStore(nil, version)
	if err != nil {
		return nil
	}
	if version > math.MaxUint32 {
		return fmt.Errorf("version overflows uint32: %d", version)
	}

	storesKeys := make([]types.StoreKey, 0, len(rs.storesParams))
	for key := range rs.storesParams {
		storesKeys = append(storesKeys, key)
	}
	// deterministic iteration order for upgrades
	sort.Slice(storesKeys, func(i, j int) bool {
		return storesKeys[i].Name() < storesKeys[j].Name()
	})

	initialStores := make([]string, 0, len(storesKeys))
	for _, key := range storesKeys {
		if rs.storesParams[key].typ == types.StoreTypeIAVL {
			initialStores = append(initialStores, key.Name())
		}
	}
	rs.scStore.Initialize(initialStores)
	if _, err := rs.scStore.LoadVersion(version, false); err != nil {
		return nil
	}

	tree := rs.scStore.GetTreeByName("chainservice_0")
	namespaceKeys, err := rs.MountStore(tree, version)
	if err != nil {
		return nil
	}
	if namespaceKeys != nil && len(namespaceKeys) > 0 {
		storesKeys = namespaceKeys
	}
	var treeUpgrades []*proto.TreeNameUpgrade
	for _, key := range storesKeys {
		switch {
		case upgrades.IsDeleted(key.Name()):
			treeUpgrades = append(treeUpgrades, &proto.TreeNameUpgrade{Name: key.Name(), Delete: true})
			rs.removalMap[key] = true
		case upgrades.IsAdded(key.Name()) || upgrades.RenamedFrom(key.Name()) != "":
			treeUpgrades = append(treeUpgrades, &proto.TreeNameUpgrade{Name: key.Name(), RenameFrom: upgrades.RenamedFrom(key.Name())})
			oldName := upgrades.RenamedFrom(key.Name())
			if oldName != "" {
				oldKey := types.NewKVStoreKey(oldName)
				rs.removalMap[oldKey] = true
			}

		}
	}

	if len(treeUpgrades) > 0 {
		if err := rs.scStore.ApplyUpgrades(treeUpgrades); err != nil {
			return err
		}
	}
	newStores := make(map[types.StoreKey]types.CommitKVStore, len(storesKeys))
	for _, key := range storesKeys {
		newStores[key], err = rs.loadCommitStoreFromParams(key, rs.storesParams[key])
		if err != nil {
			return err
		}
	}

	rs.mtx.Lock()
	defer rs.mtx.Unlock()
	rs.ckvStores = newStores
	// to keep the root hash compatible with cosmos-sdk 0.46
	if rs.scStore.Version() != 0 {
		rs.lastCommitInfo = convertCommitInfo(rs.scStore.LastCommitInfo())
		rs.lastCommitInfo = amendCommitInfo(rs.lastCommitInfo, rs.storesParams)
	} else {
		rs.lastCommitInfo = &types.CommitInfo{}
	}
	return nil
}

func (rs *Store) loadCommitStoreFromParams(key types.StoreKey, params storeParams) (types.CommitKVStore, error) {
	switch params.typ {
	case types.StoreTypeMulti:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeIAVL:
		tree := rs.scStore.GetTreeByName(key.Name())
		if tree == nil {
			return nil, fmt.Errorf("new store is not added in upgrades: %s", key.Name())
		}
		hash := tree.RootHash()
		if hash == nil {
			upgrade := proto.TreeNameUpgrade{
				Name: key.Name(),
			}
			rs.scStore.ApplyUpgrades([]*proto.TreeNameUpgrade{&upgrade})
			tree = rs.scStore.GetTreeByName(key.Name())
		}
		store := commitment.NewStore(tree, rs.logger)
		if rs.interBlockCache != nil {
			// Wrap and get a CommitKVStore with inter-block caching. Note, this should
			// only wrap the primary CommitKVStore, not any store that is already
			// branched as that will create unexpected behavior.
			newStore := rs.interBlockCache.GetStoreCache(key, store)
			return types.CommitKVStore(newStore), nil
		}

		return types.CommitKVStore(store), nil
	case types.StoreTypeDB:
		panic("recursive MultiStores not yet supported")
	case types.StoreTypeTransient:
		_, ok := key.(*types.TransientStoreKey)
		if !ok {
			return nil, fmt.Errorf("invalid StoreKey for StoreTypeTransient: %s", key.String())
		}
		return transient.NewStore(), nil
	case types.StoreTypeMemory:
		if _, ok := key.(*types.MemoryStoreKey); !ok {
			return nil, fmt.Errorf("unexpected key type for a MemoryStoreKey; got: %s", key.String())
		}
		return mem.NewStore(), nil

	default:
		panic(fmt.Sprintf("unrecognized store type %v", params.typ))
	}
}

// Implements interface CommitMultiStore
// used by export cmd
func (rs *Store) LoadVersion(ver int64) error {
	return rs.LoadVersionAndUpgrade(ver, nil)
}

// SetInterBlockCache is a noop since we do caching on its own, which works well with zero-copy.
func (rs *Store) SetInterBlockCache(inter types.MultiStorePersistentCache) {
	rs.interBlockCache = inter
}

// SetInitialVersion Implements interface CommitMultiStore
// used by InitChain when the initial height is bigger than 1
func (rs *Store) SetInitialVersion(version int64) error {
	return rs.scStore.SetInitialVersion(version)
}

// Implements interface CommitMultiStore
func (rs *Store) SetIAVLCacheSize(_ int) {
}

// Implements interface CommitMultiStore
func (rs *Store) SetIAVLDisableFastNode(_ bool) {
}

// Implements interface CommitMultiStore
func (rs *Store) SetLazyLoading(_ bool) {
}

// RollbackToVersion delete the versions after `target` and update the latest version.
// it should only be called in standalone cli commands.
func (rs *Store) RollbackToVersion(target int64) error {
	if target <= 0 {
		return fmt.Errorf("invalid rollback height target: %d", target)
	}

	if target > math.MaxUint32 {
		return fmt.Errorf("rollback height target %d exceeds max uint32", target)
	}
	err := rs.scStore.Rollback(target)
	if err != nil {
		return err
	}
	return rs.LoadVersionAndUpgrade(0, nil)
}

// getStoreByName performs a lookup of a StoreKey given a store name typically
// provided in a path. The StoreKey is then used to perform a lookup and return
// a Store. If the Store is wrapped in an inter-block cache, it will be unwrapped
// prior to being returned. If the StoreKey does not exist, nil is returned.
func (rs *Store) GetStoreByName(name string) types.Store {
	key := rs.storeKeys[name]
	if key == nil {
		return nil
	}

	return rs.GetCommitKVStore(key)
}

// Implements interface Queryable
func (rs *Store) Query(req *types.RequestQuery) (*types.ResponseQuery, error) {
	version := req.Height
	if version <= 0 || version > rs.lastCommitInfo.Version {
		version = rs.scStore.Version()
	}
	path := req.Path
	storeName, subPath, err := parsePath(path)
	if err != nil {
		return &types.ResponseQuery{}, err
	}
	var store types.Queryable
	var commitInfo *types.CommitInfo

	if !req.Prove && rs.ssStore != nil {
		// Serve abci query from ss store if no proofs needed
		store = types.Queryable(state.NewStore(rs.ssStore, types.NewKVStoreKey(storeName), version))
	} else {
		// Serve abci query from historical sc store if proofs needed
		scStore, err := rs.scStore.LoadVersion(version, true)
		if err != nil {
			return &types.ResponseQuery{}, err
		}
		defer scStore.Close()
		store = types.Queryable(commitment.NewStore(scStore.GetTreeByName(storeName), rs.logger))
		commitInfo = convertCommitInfo(scStore.LastCommitInfo())
		commitInfo = amendCommitInfo(commitInfo, rs.storesParams)
	}

	// trim the path and execute the query
	req.Path = subPath
	res, err := store.Query(req)
	if err != nil {
		return &types.ResponseQuery{}, err
	}

	if !req.Prove || !rootmulti.RequireProof(subPath) {
		return res, nil
	} else if commitInfo != nil {
		// Restore origin path and append proof op.
		res.ProofOps.Ops = append(res.ProofOps.Ops, commitInfo.ProofOp(storeName))
	}
	if res.ProofOps == nil || len(res.ProofOps.Ops) == 0 {
		return &types.ResponseQuery{}, errors.Wrap(sdkerrors.ErrInvalidRequest, "proof is unexpectedly empty; ensure height has not been pruned")
	}
	return res, nil
}

// parsePath expects a format like /<storeName>[/<subpath>]
// Must start with /, subpath may be empty
// Returns error if it doesn't start with /
func parsePath(path string) (storeName string, subpath string, err error) {
	if !strings.HasPrefix(path, "/") {
		return storeName, subpath, errors.Wrapf(sdkerrors.ErrUnknownRequest, "invalid path: %s", path)
	}

	paths := strings.SplitN(path[1:], "/", 2)
	storeName = paths[0]

	if len(paths) == 2 {
		subpath = "/" + paths[1]
	}

	return storeName, subpath, nil
}

type storeParams struct {
	key types.StoreKey
	typ types.StoreType
}

func newStoreParams(key types.StoreKey, typ types.StoreType) storeParams {
	return storeParams{
		key: key,
		typ: typ,
	}
}

func mergeStoreInfos(commitInfo *types.CommitInfo, storeInfos []types.StoreInfo) *types.CommitInfo {
	infos := make([]types.StoreInfo, 0, len(commitInfo.StoreInfos)+len(storeInfos))
	infos = append(infos, commitInfo.StoreInfos...)
	infos = append(infos, storeInfos...)
	sort.SliceStable(infos, func(i, j int) bool {
		return infos[i].Name < infos[j].Name
	})
	return &types.CommitInfo{
		Version:    commitInfo.Version,
		StoreInfos: infos,
	}
}

// amendCommitInfo add mem stores commit infos to keep it compatible with cosmos-sdk 0.46
func amendCommitInfo(commitInfo *types.CommitInfo, storeParams map[types.StoreKey]storeParams) *types.CommitInfo {
	var extraStoreInfos []types.StoreInfo
	for key := range storeParams {
		typ := storeParams[key].typ
		if typ != types.StoreTypeIAVL && typ != types.StoreTypeTransient {
			extraStoreInfos = append(extraStoreInfos, types.StoreInfo{
				Name:     key.Name(),
				CommitId: types.CommitID{},
			})
		}
	}
	return mergeStoreInfos(commitInfo, extraStoreInfos)
}

func convertCommitInfo(commitInfo *proto.CommitInfo) *types.CommitInfo {
	storeInfos := make([]types.StoreInfo, len(commitInfo.StoreInfos))
	for i, storeInfo := range commitInfo.StoreInfos {
		storeInfos[i] = types.StoreInfo{
			Name: storeInfo.Name,
			CommitId: types.CommitID{
				Version: storeInfo.CommitId.Version,
				Hash:    storeInfo.CommitId.Hash,
			},
		}
	}
	return &types.CommitInfo{
		Version:    commitInfo.Version,
		StoreInfos: storeInfos,
	}
}

// GetWorkingHash returns the working app hash
func (rs *Store) GetWorkingHash() ([]byte, error) {
	t1 := time.Now()
	if err := rs.flush(); err != nil {
		return nil, err
	}
	t2 := time.Now()
	info := rs.scStore.WorkingCommitInfo()
	t25 := time.Now()

	commitInfo := convertCommitInfo(info)
	t3 := time.Now()

	// for sdk 0.46 and backward compatibility
	commitInfo = amendCommitInfo(commitInfo, rs.storesParams)
	t4 := time.Now()
	println("get working hash", "flush time", t2.Sub(t1).String(), "working commit info", t25.Sub(t2).String(), "convert commit info", t3.Sub(t25).String(), "amend commit info", t4.Sub(t3).String())
	return commitInfo.Hash(), nil
}

func (rs *Store) GetEvents() []abci.Event {
	panic("should never attempt to get events from commit multi store")
}

func (rs *Store) ResetEvents() {
	panic("should never attempt to reset events from commit multi store")
}

// ListeningEnabled will always return false for seiDB
func (rs *Store) ListeningEnabled(_ types.StoreKey) bool {
	return false
}

//// AddListeners is no-opts for seiDB
//func (rs *Store) AddListeners(_ types.StoreKey, _ []types.WriteListener) {
//	return
//}

// Restore Implements interface Snapshotter
func (rs *Store) Restore1(
	height uint64, format uint32, protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	if rs.scStore != nil {
		if err := rs.scStore.Close(); err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("failed to close db: %w", err)
		}
	}
	item, err := rs.restore(int64(height), protoReader)
	if err != nil {
		return snapshottypes.SnapshotItem{}, err
	}

	return item, rs.LoadLatestVersion()
}

func (rs *Store) restore(height int64, protoReader protoio.Reader) (snapshottypes.SnapshotItem, error) {
	var (
		ssImporter   chan sstypes.SnapshotNode
		snapshotItem snapshottypes.SnapshotItem
		storeKey     string
		restoreErr   error
	)
	scImporter, err := rs.scStore.Importer(height)
	if err != nil {
		return snapshottypes.SnapshotItem{}, err
	}
	if rs.ssStore != nil {
		ssImporter = make(chan sstypes.SnapshotNode, 10000)
		go func() {
			err := rs.ssStore.Import(height, ssImporter)
			if err != nil {
				panic(err)
			}
		}()
	}
loop:
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err = protoReader.ReadMsg(&snapshotItem)
		if err == io.EOF {
			break
		} else if err != nil {
			restoreErr = errors.Wrap(err, "invalid protobuf message")
			break loop
		}

		switch item := snapshotItem.Item.(type) {
		case *snapshottypes.SnapshotItem_Store:
			storeKey = item.Store.Name
			if err = scImporter.AddTree(storeKey); err != nil {
				restoreErr = err
				break loop
			}
			rs.logger.Info(fmt.Sprintf("Start restoring store: %s", storeKey))
		case *snapshottypes.SnapshotItem_IAVL:
			if item.IAVL.Height > math.MaxInt8 {
				restoreErr = errors.Wrapf(sdkerrors.ErrLogic, "node height %v cannot exceed %v",
					item.IAVL.Height, math.MaxInt8)
				break loop
			}
			node := &sctypes.SnapshotNode{
				Key:     item.IAVL.Key,
				Value:   item.IAVL.Value,
				Height:  int8(item.IAVL.Height),
				Version: item.IAVL.Version,
			}
			// Protobuf does not differentiate between []byte{} as nil, but fortunately IAVL does
			// not allow nil keys nor nil values for leaf nodes, so we can always set them to empty.
			if node.Key == nil {
				node.Key = []byte{}
			}
			if node.Height == 0 && node.Value == nil {
				node.Value = []byte{}
			}
			scImporter.AddNode(node)

			// Check if we should also import to SS store
			if rs.ssStore != nil && node.Height == 0 && ssImporter != nil {
				ssImporter <- sstypes.SnapshotNode{
					StoreKey: storeKey,
					Key:      node.Key,
					Value:    node.Value,
				}
			}
		default:
			// unknown element, could be an extension
			break loop
		}
	}

	if err = scImporter.Close(); err != nil {
		if restoreErr == nil {
			restoreErr = err
		}
	}
	if ssImporter != nil {
		close(ssImporter)
	}

	return snapshotItem, restoreErr
}

// Snapshot Implements the interface from Snapshotter
func (rs *Store) Snapshot1(height uint64, protoWriter protoio.Writer) error {
	if height > math.MaxUint32 {
		return fmt.Errorf("height overflows uint32: %d", height)
	}

	exporter, err := rs.scStore.Exporter(int64(height))
	if err != nil {
		return err
	}
	defer exporter.Close()
	keySizePerStore := map[string]int64{}
	valueSizePerStore := map[string]int64{}
	numKeysPerStore := map[string]int64{}
	currentStoreName := ""
	for {
		item, err := exporter.Next()
		if err != nil {
			if err == commonerrors.ErrorExportDone {
				for k, v := range keySizePerStore {
					label := hashmetrics.Label{"store_name", k}
					telemetry.SetGaugeWithLabels(
						[]string{"iavl", "store", "total_key_bytes"},
						float32(v),
						[]hashmetrics.Label{label},
					)
				}
				for k, v := range valueSizePerStore {
					telemetry.SetGaugeWithLabels(
						[]string{"iavl", "store", "total_value_bytes"},
						float32(v),
						[]hashmetrics.Label{telemetry.NewLabel("store_name", k)},
					)
				}
				for k, v := range numKeysPerStore {
					telemetry.SetGaugeWithLabels(
						[]string{"iavl", "store", "total_num_keys"},
						float32(v),
						[]hashmetrics.Label{telemetry.NewLabel("store_name", k)},
					)
				}
				break
			}
			return err
		}
		switch item := item.(type) {
		case *sctypes.SnapshotNode:
			if err := protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
				Item: &snapshottypes.SnapshotItem_IAVL{
					IAVL: &snapshottypes.SnapshotIAVLItem{
						Key:     item.Key,
						Value:   item.Value,
						Height:  int32(item.Height),
						Version: item.Version,
					},
				},
			}); err != nil {
				return err
			}
			keySizePerStore[currentStoreName] += int64(len(item.Key))
			valueSizePerStore[currentStoreName] += int64(len(item.Value))
			numKeysPerStore[currentStoreName] += 1
		case string:
			if err := protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
				Item: &snapshottypes.SnapshotItem_Store{
					Store: &snapshottypes.SnapshotStoreItem{
						Name: item,
					},
				},
			}); err != nil {
				return err
			}
			currentStoreName = item
		default:
			return fmt.Errorf("unknown item type %T", item)
		}
	}

	return nil
}

// SetKVStores implements types.CommitMultiStore.
func (*Store) SetKVStores(handler func(key types.StoreKey, s types.KVStore) types.CacheWrap) types.MultiStore {
	panic("unimplemented")
}

// StoreKeys implements types.CommitMultiStore.
func (rs *Store) StoreKeys() []types.StoreKey {
	res := make([]types.StoreKey, len(rs.storeKeys))
	for _, sk := range rs.storeKeys {
		res = append(res, sk)
	}
	return res
}

// GetEarliestVersion return earliest version for SS or latestVersion if only SC is enabled
func (rs *Store) GetEarliestVersion() int64 {
	latestVersion := rs.lastCommitInfo.Version
	if rs.ssStore != nil {
		version, err := rs.ssStore.GetEarliestVersion()
		if err != nil {
			return latestVersion
		}
		return version
	}
	return latestVersion
}

func (st *Store) GetParentStore() types.CommitKVStore {
	return nil
}
