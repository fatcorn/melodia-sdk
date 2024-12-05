package baseapp

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/cockroachdb/errors"
	cmtdbm "github.com/cometbft/cometbft-db"
	abci "github.com/cometbft/cometbft/abci/types"
	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	tmbytes "github.com/cometbft/cometbft/libs/bytes"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/gogoproto/proto"
	"golang.org/x/exp/maps"
	protov2 "google.golang.org/protobuf/proto"
	"math"
	"math/big"
	"sort"
	"strconv"
	"sync"
	"time"

	"cosmossdk.io/core/header"
	errorsmod "cosmossdk.io/errors"
	"cosmossdk.io/log"
	"cosmossdk.io/store"
	storemetrics "cosmossdk.io/store/metrics"
	"cosmossdk.io/store/snapshots"
	storetypes "cosmossdk.io/store/types"

	"encoding/json"
	"github.com/cometbft/cometbft/crypto/merkle"
	bfttypes "github.com/cometbft/cometbft/types"
	"github.com/cosmos/cosmos-sdk/baseapp/oe"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	"github.com/cosmos/cosmos-sdk/nsblockstore"
	servertypes "github.com/cosmos/cosmos-sdk/server/types"
	rootmultiv2 "github.com/cosmos/cosmos-sdk/storev2/rootmulti"
	"github.com/cosmos/cosmos-sdk/telemetry"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/types/mempool"
	"github.com/near/borsh-go"
	"golang.org/x/exp/slices"
)

type (
	execMode uint8

	// StoreLoader defines a customizable function to control how we load the
	// CommitMultiStore from disk. This is useful for state migration, when
	// loading a datastore written with an older version of the software. In
	// particular, if a module changed the substore key name (or removed a substore)
	// between two versions of the software.
	StoreLoader func(ms storetypes.CommitMultiStore) error

	DeliverTxHook func(sdk.Context, sdk.Tx, [32]byte, *abci.ExecTxResult)
)

const (
	execModeCheck               execMode = iota // Check a transaction
	execModeReCheck                             // Recheck a (pending) transaction after a commit
	execModeSimulate                            // Simulate a transaction
	execModePrepareProposal                     // Prepare a block proposal
	execModeProcessProposal                     // Process a block proposal
	execModeVoteExtension                       // Extend or verify a pre-commit vote
	execModeVerifyVoteExtension                 // Verify a vote extension
	execModeFinalize                            // Finalize a block proposal
)

var _ servertypes.ABCI = (*BaseApp)(nil)

// BaseApp reflects the ABCI application implementation.
type BaseApp struct {
	// initialized on creation
	logger            log.Logger
	name              string                      // application name from abci.BlockInfo
	db                dbm.DB                      // common DB backend
	cms               storetypes.CommitMultiStore // Main (uncached) state
	qms               storetypes.MultiStore       // Optional alternative multistore for querying only.
	storeLoader       StoreLoader                 // function to handle store loading, may be overridden with SetStoreLoader()
	grpcQueryRouter   *GRPCQueryRouter            // router for redirecting gRPC query calls
	msgServiceRouter  *MsgServiceRouter           // router for redirecting Msg service messages
	interfaceRegistry codectypes.InterfaceRegistry
	txDecoder         sdk.TxDecoder // unmarshal []byte into sdk.Tx
	txEncoder         sdk.TxEncoder // marshal sdk.Tx into []byte

	mempool     mempool.Mempool // application side mempool
	anteHandler sdk.AnteHandler // ante handler for fee and auth
	postHandler sdk.PostHandler // post handler, optional

	pibcPacketGeneratorHadler *sdk.PIBCPacketGeneratorHandler

	initChainer            sdk.InitChainer  // ABCI InitChain handler
	preBlocker             sdk.PreBlocker   // logic to run before BeginBlocker
	beginBlocker           sdk.BeginBlocker // (legacy ABCI) BeginBlock handler
	internalEventDetector  sdk.InternalEventDetector
	endBlocker             sdk.EndBlocker // (legacy ABCI) EndBlock handler
	namespaceBlockPreparer sdk.NamespaceBlockPreparer
	namespaceBlockMaker    sdk.NamespaceMakeBlock
	processProposal        sdk.ProcessProposalHandler     // ABCI ProcessProposal handler
	prepareProposal        sdk.PrepareProposalHandler     // ABCI PrepareProposal
	extendVote             sdk.ExtendVoteHandler          // ABCI ExtendVote handler
	verifyVoteExt          sdk.VerifyVoteExtensionHandler // ABCI VerifyVoteExtension handler
	prepareCheckStater     sdk.PrepareCheckStater         // logic to run during commit using the checkState
	precommiter            sdk.Precommiter                // logic to run during commit using the deliverState

	addrPeerFilter sdk.PeerFilter // filter peers by address and port
	idPeerFilter   sdk.PeerFilter // filter peers by node ID
	fauxMerkleMode bool           // if true, IAVL MountStores uses MountStoresDB for simulation speed.

	// manages snapshots, i.e. dumps of app state at certain intervals
	snapshotManager *snapshots.Manager

	// volatile states:
	//
	// - checkState is set on InitChain and reset on Commit
	// - finalizeBlockState is set on InitChain and FinalizeBlock and set to nil
	// on Commit.
	//
	// - checkState: Used for CheckTx, which is set based on the previous block's
	// state. This state is never committed.
	//
	// - prepareProposalState: Used for PrepareProposal, which is set based on the
	// previous block's state. This state is never committed. In case of multiple
	// consensus rounds, the state is always reset to the previous block's state.
	//
	// - processProposalState: Used for ProcessProposal, which is set based on the
	// the previous block's state. This state is never committed. In case of
	// multiple rounds, the state is always reset to the previous block's state.
	//
	// - finalizeBlockState: Used for FinalizeBlock, which is set based on the
	// previous block's state. This state is committed.
	checkState           *state
	prepareProposalState *state
	processProposalState *state
	finalizeBlockState   *state
	finalizeBlockHeight  int64

	// An inter-block write-through cache provided to the context during the ABCI
	// FinalizeBlock call.
	interBlockCache storetypes.MultiStorePersistentCache

	// paramStore is used to query for ABCI consensus parameters from an
	// application parameter store.
	paramStore ParamStore

	// queryGasLimit defines the maximum gas for queries; unbounded if 0.
	queryGasLimit uint64

	// The minimum gas prices a validator is willing to accept for processing a
	// transaction. This is mainly used for DoS and spam prevention.
	minGasPrices sdk.DecCoins

	// initialHeight is the initial height at which we start the BaseApp
	initialHeight int64

	// flag for sealing options and parameters to a BaseApp
	sealed bool

	// block height at which to halt the chain and gracefully shutdown
	haltHeight uint64

	// minimum block time (in Unix seconds) at which to halt the chain and gracefully shutdown
	haltTime uint64

	// minRetainBlocks defines the minimum block height offset from the current
	// block being committed, such that all blocks past this offset are pruned
	// from CometBFT. It is used as part of the process of determining the
	// ResponseCommit.RetainHeight value during ABCI Commit. A value of 0 indicates
	// that no blocks should be pruned.
	//
	// Note: CometBFT block pruning is dependant on this parameter in conjunction
	// with the unbonding (safety threshold) period, state pruning and state sync
	// snapshot parameters to determine the correct minimum value of
	// ResponseCommit.RetainHeight.
	minRetainBlocks uint64

	// application's version string
	version string

	// application's protocol version that increments on every upgrade
	// if BaseApp is passed to the upgrade keeper's NewKeeper method.
	appVersion uint64

	// recovery handler for app.runTx method
	runTxRecoveryMiddleware recoveryMiddleware

	// trace set will return full stack traces for errors in ABCI Log field
	trace bool

	// indexEvents defines the set of events in the form {eventType}.{attributeKey},
	// which informs CometBFT what to index. If empty, all events will be indexed.
	indexEvents map[string]struct{}

	// streamingManager for managing instances and configuration of ABCIListener services
	streamingManager storetypes.StreamingManager

	chainID string

	cdc codec.Codec

	// optimisticExec contains the context required for Optimistic Execution,
	// including the goroutine handling.This is experimental and must be enabled
	// by developers.
	optimisticExec *oe.OptimisticExecution

	// disableBlockGasMeter will disable the block gas meter if true, block gas meter is tricky to support
	// when executing transactions in parallel.
	// when disabled, the block gas meter in context is a noop one.
	//
	// SAFETY: it's safe to do if validators validate the total gas wanted in the `ProcessProposal`, which is the case in the default handler.
	disableBlockGasMeter bool

	NamespaceStoresLoaded bool

	namespaceStoreManager *nsblockstore.NamespaceStoreManager

	signerKeyring keyring.Keyring

	signerName string

	writeDeferredBalancesHandler               sdk.WriteDeferredBalancesHandler
	addCosmosEventsToEVMReceiptIfApplicable    sdk.AddCosmosEventsToEVMReceiptIfApplicable
	addTransientGasWanted                      sdk.AddTransientGasWanted
	modifyLogIndexHandler                      sdk.ModifyLogIndexHandler
	setBlockBloomTransientHandler              sdk.SetBlockBloomTransientHandler
	contractStaticAnalysisResultHandler        sdk.ContractStaticAnalysisResultHandler
	isEVMTxHandler                             sdk.IsEVMTxHandler
	txAccountInfoHandler                       sdk.TxAccountInfoHandler
	nextAccountNumberHandler                   sdk.NextAccountNumberHandler
	txNewAccountAddressesHandler               sdk.TxNewAccountAddressesHandler
	crossNamespaceTransactionIdentifierHandler sdk.CrossNamespaceTransactionIdentifierHandler
	getAccountNonceHandler                     sdk.GetAccountNonceHandler
	baseFeeHandler                             sdk.BaseFeeHandler
	checkTxFeeHandler                          sdk.CheckTxFeeHandler

	deliverTxHooks     []DeliverTxHook
	concurrencyWorkers int
}

//type NamespaceStoreManager struct {
//	nsStores               map[uint64]nsblockstore.NSBlockStore
//	namespaceIdsMoudle     map[uint64]string
//	moudleRelatedMoudles   map[string][]string
//	dbRootDir              string
//	namespaceActivateState map[uint64]bool
//}
//
//func newNamespaceStoreManager() NamespaceStoreManager {
//	return NamespaceStoreManager{
//		nsStores:               make(map[uint64]nsblockstore.NSBlockStore),
//		namespaceIdsMoudle:     make(map[uint64]string),
//		moudleRelatedMoudles:   make(map[string][]string),
//		namespaceActivateState: make(map[uint64]bool),
//	}
//}
//
//func (manager *NamespaceStoreManager) GetNamespaceIdsMoudle() map[uint64]string {
//	return manager.namespaceIdsMoudle
//}
//
//func (manager *NamespaceStoreManager) GetNamespacensStores() map[uint64]nsblockstore.NSBlockStore {
//	return manager.nsStores
//}
//
//func (manager *NamespaceStoreManager) GetMoudleRelatedMoudles() map[string][]string {
//	return manager.moudleRelatedMoudles
//}
//
//type NamespaceStoreDBInfo struct {
//	NamespaceStoreDBs    map[uint64]cmtdbm.DB
//	DbRootDir            string
//	NamespacesModuleName map[uint64]string
//	MoudleRelatedMoudles map[string][]string
//}

// NewBaseApp returns a reference to an initialized BaseApp. It accepts a
// variadic number of option functions, which act on the BaseApp to set
// configuration choices.
func NewBaseApp(
	name string, logger log.Logger, db dbm.DB, txDecoder sdk.TxDecoder, options ...func(*BaseApp),
) *BaseApp {
	app := &BaseApp{
		logger:                logger,
		name:                  name,
		db:                    db,
		cms:                   store.NewCommitMultiStore(db, logger, storemetrics.NewNoOpMetrics()), // by default we use a no-op metric gather in store
		storeLoader:           DefaultStoreLoader,
		grpcQueryRouter:       NewGRPCQueryRouter(),
		msgServiceRouter:      NewMsgServiceRouter(),
		txDecoder:             txDecoder,
		fauxMerkleMode:        false,
		queryGasLimit:         math.MaxUint64,
		namespaceStoreManager: nsblockstore.NewNamespaceStoreManager(),
	}

	for _, option := range options {
		option(app)
	}

	if app.mempool == nil {
		app.SetMempool(mempool.NoOpMempool{})
	}

	abciProposalHandler := NewDefaultProposalHandler(app.mempool, app)

	if app.prepareProposal == nil {
		app.SetPrepareProposal(abciProposalHandler.PrepareProposalHandler())
	}
	if app.processProposal == nil {
		app.SetProcessProposal(abciProposalHandler.ProcessProposalHandler())
	}
	if app.extendVote == nil {
		app.SetExtendVoteHandler(NoOpExtendVote())
	}
	if app.verifyVoteExt == nil {
		app.SetVerifyVoteExtensionHandler(NoOpVerifyVoteExtensionHandler())
	}
	if app.interBlockCache != nil {
		app.cms.SetInterBlockCache(app.interBlockCache)
	}

	app.runTxRecoveryMiddleware = newDefaultRecoveryMiddleware()

	// Initialize with an empty interface registry to avoid nil pointer dereference.
	// Unless SetInterfaceRegistry is called with an interface registry with proper address codecs baseapp will panic.
	app.cdc = codec.NewProtoCodec(codectypes.NewInterfaceRegistry())
	id := app.cms.LastCommitID()
	app.finalizeBlockHeight = id.Version

	return app
}

// Name returns the name of the BaseApp.
func (app *BaseApp) Name() string {
	return app.name
}

// AppVersion returns the application's protocol version.
func (app *BaseApp) AppVersion() uint64 {
	return app.appVersion
}

// Version returns the application's version string.
func (app *BaseApp) Version() string {
	return app.version
}

// Logger returns the logger of the BaseApp.
func (app *BaseApp) Logger() log.Logger {
	return app.logger
}

// Trace returns the boolean value for logging error stack traces.
func (app *BaseApp) Trace() bool {
	return app.trace
}

// MsgServiceRouter returns the MsgServiceRouter of a BaseApp.
func (app *BaseApp) MsgServiceRouter() *MsgServiceRouter { return app.msgServiceRouter }

// SetMsgServiceRouter sets the MsgServiceRouter of a BaseApp.
func (app *BaseApp) SetMsgServiceRouter(msgServiceRouter *MsgServiceRouter) {
	app.msgServiceRouter = msgServiceRouter
}

// MountStores mounts all IAVL or DB stores to the provided keys in the BaseApp
// multistore.
func (app *BaseApp) MountStores(keys ...storetypes.StoreKey) {
	allStoreKeys := app.LoadAllValidNamespaceStore(keys...)

	for _, key := range allStoreKeys {
		switch key.(type) {
		case *storetypes.KVStoreKey:
			if !app.fauxMerkleMode {
				app.MountStore(key, storetypes.StoreTypeIAVL)
			} else {
				// StoreTypeDB doesn't do anything upon commit, and it doesn't
				// retain history, but it's useful for faster simulation.
				app.MountStore(key, storetypes.StoreTypeDB)
			}

		case *storetypes.TransientStoreKey:
			app.MountStore(key, storetypes.StoreTypeTransient)

		case *storetypes.MemoryStoreKey:
			app.MountStore(key, storetypes.StoreTypeMemory)

		default:
			panic(fmt.Sprintf("Unrecognized store key type :%T", key))
		}
	}
}

func (app *BaseApp) LoadAllValidNamespaceStore(keys ...storetypes.StoreKey) []storetypes.StoreKey {

	//prefixKey := []byte{115, 47, 107, 58, 99, 104, 97, 105, 110, 115, 101, 114, 118, 105, 99, 101, 47}
	//endKey := []byte{115, 47, 107, 58, 99, 104, 97, 105, 110, 115, 101, 114, 118, 105, 99, 101, 47, 155}
	//iter, _ := app.db.Iterator(prefixKey, endKey)
	//time := 0
	var namespaceStores []storetypes.StoreKey
	if !app.NamespaceStoresLoaded {
		nsPorts := ListDBNamespace(app.db)
		namespaceStores = NamespaceStoresFromNSPort(nsPorts, nil)
		app.NamespaceStoresLoaded = true
	}

	defaultNamesapceKeys := make([]storetypes.StoreKey, 0)
	for _, key := range keys {
		switch key.(type) {
		case *storetypes.KVStoreKey:
			defaultNamesapceKey := storetypes.NewKVStoreKey(key.Name() + "_" + strconv.FormatUint(merkle.DefaultTxNamespaceID, 10))
			defaultNamesapceKeys = append(defaultNamesapceKeys, defaultNamesapceKey)
		case *storetypes.TransientStoreKey:
			defaultNamesapceKey := storetypes.NewTransientStoreKey(key.Name() + "_" + strconv.FormatUint(merkle.DefaultTxNamespaceID, 10))
			defaultNamesapceKeys = append(defaultNamesapceKeys, defaultNamesapceKey)
		case *storetypes.MemoryStoreKey:
			defaultNamesapceKey := storetypes.NewMemoryStoreKey(key.Name() + "_" + strconv.FormatUint(merkle.DefaultTxNamespaceID, 10))
			defaultNamesapceKeys = append(defaultNamesapceKeys, defaultNamesapceKey)
		}
	}

	allStoreKeys := append(defaultNamesapceKeys, namespaceStores...)
	return allStoreKeys
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

// ListDBNamespace list registered namespace in chainservice
func ListDBNamespace(db dbm.DB) []*bfttypes.NamespacePort {
	var nsPorts []*bfttypes.NamespacePort

	namesapceIdPrefixKey := []byte("s/k:chainservice_0/fnamespace_store")
	iter, _ := db.Iterator(namesapceIdPrefixKey, storetypes.PrefixEndBytes(namesapceIdPrefixKey))

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

// NewNamespaceDB handle internal Event to execute related logic
func CreateNamespaceDB(namespaceId uint64, moudleName string, dbRootDir string) (cmtdbm.DB, error) {
	namespaceDB, err := cmtcfg.DefaultDBProvider(&cmtcfg.DBContext{
		ID:     "namespace" + "_" + strconv.FormatUint(namespaceId, 10) + "_" + moudleName,
		Config: cmtcfg.DefaultConfig().SetRoot(dbRootDir).AddDBPath("namespace"),
	})
	return namespaceDB, err
}

// addNewNamespaceBLockStore Create a new namespace blockdb
func (app *BaseApp) addNewNamespaceBLockStore(namespaceID uint64, moduleName string, moduleRelatedModules []string) {
	newNamespaceDB, err := CreateNamespaceDB(namespaceID, moduleName, app.namespaceStoreManager.GetDBRootDir())
	if err != nil {
		panic(fmt.Sprintf("Create namespace-%v DB failed, causev%v", namespaceID, err))
	}
	nsBLockStore := nsblockstore.NewNamespaceBlockStore(newNamespaceDB, namespaceID, app.logger)
	app.namespaceStoreManager.AddNamespaceStore(namespaceID, moduleName, nsBLockStore)
	app.namespaceStoreManager.AddNamespaceIdsModule(namespaceID, moduleName)
	app.namespaceStoreManager.AddModuleRelatedModules(moduleName, moduleRelatedModules)
}

// loadNamespaceBLockStore Load a namespace blockdb to namespace store manager
func (app *BaseApp) loadNamespaceBLockStore(namespaceID uint64, db cmtdbm.DB, moduleName string, moduleRelatedModules []string) {
	nsBLockStore := nsblockstore.NewNamespaceBlockStore(db, namespaceID, app.logger)
	app.namespaceStoreManager.AddNamespaceStore(namespaceID, moduleName, nsBLockStore)
	app.namespaceStoreManager.AddNamespaceIdsModule(namespaceID, moduleName)
	app.namespaceStoreManager.AddModuleRelatedModules(moduleName, moduleRelatedModules)
	app.namespaceStoreManager.AddNamespaceActiveState(namespaceID, moduleName, nsBLockStore.GetNamespaceState())
}

// activeNamespace Create a new namespace blockdb
func (app *BaseApp) activeNamespace(namespaceID uint64, moduleName string) {
	nsBlockStore, ok := app.namespaceStoreManager.GetNamespaceStore(namespaceID, moduleName)
	if !ok {
		panic("not found namespace block store")
	}
	nsBlockStore.SetNamespaceStateActive()
	app.namespaceStoreManager.AddNamespaceActiveState(namespaceID, moduleName, true)
}

// HandleInternalEvent handle internal Event to execute related logic
func (app *BaseApp) HandleInternalEvent(events sdk.Events) {
	for _, event := range events {
		switch event.Type {
		case InternalEventTypeNewNamespace:
			{
				nsPortObj, hasKey := event.GetAttribute(InternalEventTypeNamespaceAttribute)
				if !hasKey {
					panic("Get InternalEventTypeNewNamespace but no value")
				}
				var nsPort bfttypes.NamespacePort
				err := json.Unmarshal([]byte(nsPortObj.Value), &nsPort)
				if err != nil {
					panic(fmt.Sprintf("Get InternalEventTypeNewNamespace but unmarshal value failed %v", err))
				}
				newNamespaceStoreKey := NamespaceStoresFromNSPort([]*bfttypes.NamespacePort{&nsPort}, nil)
				err = app.cms.DynamicAddNamesapceStore(newNamespaceStoreKey...)
				if err != nil {
					panic(fmt.Sprintf("Dynamic add namesapce store failed %v", err))
				}
			}
		case InternalEventTypeNamespaceActive:
			{
				nsPortObj, hasKey := event.GetAttribute(InternalEventTypeNamespaceAttribute)
				if !hasKey {
					panic("Get InternalEventTypeNamespaceActive but no value")
				}
				var nsPort bfttypes.NamespacePort
				err := json.Unmarshal([]byte(nsPortObj.Value), &nsPort)
				if err != nil {
					panic(fmt.Sprintf("Get InternalEventTypeNewNamespace but unmarshal value failed %v", err))
				}
				app.addNewNamespaceBLockStore(uint64(nsPort.NamespaceId), nsPort.Module, nsPort.ListAllDependentMoudlesName())
				app.activeNamespace(uint64(nsPort.NamespaceId), nsPort.Module)
			}
		case InternalEventTypeStartNamespaceService:
			{
				nsPortObj, hasKey := event.GetAttribute(InternalEventTypeNamespaceAttribute)
				if !hasKey {
					panic("Get InternalEventTypeStartNamespaceService but no value")
				}
				namespaceType, hasKey := event.GetAttribute(InternalEventTypeStartNamespaceServiceTypeAttribute)
				if !hasKey {
					panic("Get InternalEventTypeStartNamespaceService but no value")
				}

				var isConsensus bool
				if namespaceType.Value == fmt.Sprintf("%v", bool(true)) {
					isConsensus = true
				}

				var nsPort bfttypes.NamespacePort
				err := json.Unmarshal([]byte(nsPortObj.Value), &nsPort)
				if err != nil {
					panic(fmt.Sprintf("Get InternalEventTypeNewNamespace but unmarshal value failed %v", err))
				}
				if isConsensus {
					bfttypes.NamespacePortWaitCh <- nsPort
				} else {
					sdk.NamespacePortWaitCh <- nsPort
				}
			}

		}

	}
}

// MountKVStores mounts all IAVL or DB stores to the provided keys in the
// BaseApp multistore.
func (app *BaseApp) MountKVStores(keys map[string]*storetypes.KVStoreKey) {
	for _, key := range keys {
		if !app.fauxMerkleMode {
			app.MountStore(key, storetypes.StoreTypeIAVL)
		} else {
			// StoreTypeDB doesn't do anything upon commit, and it doesn't
			// retain history, but it's useful for faster simulation.
			app.MountStore(key, storetypes.StoreTypeDB)
		}
	}
}

// MountTransientStores mounts all transient stores to the provided keys in
// the BaseApp multistore.
func (app *BaseApp) MountTransientStores(keys map[string]*storetypes.TransientStoreKey) {
	for _, key := range keys {
		app.MountStore(key, storetypes.StoreTypeTransient)
	}
}

// MountMemoryStores mounts all in-memory KVStores with the BaseApp's internal
// commit multi-store.
func (app *BaseApp) MountMemoryStores(keys map[string]*storetypes.MemoryStoreKey) {
	skeys := maps.Keys(keys)
	sort.Strings(skeys)
	for _, key := range skeys {
		memKey := keys[key]
		app.MountStore(memKey, storetypes.StoreTypeMemory)
	}
}

// MountStore mounts a store to the provided key in the BaseApp multistore,
// using the default DB.
func (app *BaseApp) MountStore(key storetypes.StoreKey, typ storetypes.StoreType) {
	app.cms.MountStoreWithDB(key, typ, nil)
}

// LoadLatestVersion loads the latest application version. It will panic if
// called more than once on a running BaseApp.
func (app *BaseApp) LoadLatestVersion() error {
	err := app.storeLoader(app.cms)
	if err != nil {
		return fmt.Errorf("failed to load latest version: %w", err)
	}
	cms := app.cms
	store, ok := cms.(*rootmultiv2.Store)
	if ok {
		info, err := store.LoadNamespaceStoreInfo(app.namespaceStoreManager.GetDBRootDir())
		if err != nil {
			panic(err)
		}
		for storeKey, db := range info.NamespaceStoreDBs {
			namespaceModule := info.NamespacesModuleName[storeKey]
			relatedModules := info.ModuleRelatedModules[namespaceModule.ModuleName]
			app.loadNamespaceBLockStore(namespaceModule.NamespaceId, db, namespaceModule.ModuleName, relatedModules)
		}
		app.namespaceStoreManager.SetDBRootDir(info.DbRootDir)
	}

	return app.Init()
}

// DefaultStoreLoader will be used by default and loads the latest version
func DefaultStoreLoader(ms storetypes.CommitMultiStore) error {
	return ms.LoadLatestVersion()
}

// CommitMultiStore returns the root multi-store.
// App constructor can use this to access the `cms`.
// UNSAFE: must not be used during the abci life cycle.
func (app *BaseApp) CommitMultiStore() storetypes.CommitMultiStore {
	return app.cms
}

// SnapshotManager returns the snapshot manager.
// application use this to register extra extension snapshotters.
func (app *BaseApp) SnapshotManager() *snapshots.Manager {
	return app.snapshotManager
}

// NamespaceStoreManager returns the snapshot manager.
// application use this to register extra extension snapshotters.
func (app *BaseApp) NamespaceStoreManager() *nsblockstore.NamespaceStoreManager {
	return app.namespaceStoreManager
}

// IndexEvents returns the indexEvents.
func (app *BaseApp) IndexEvents() map[string]struct{} {
	return app.indexEvents
}

// LoadVersion loads the BaseApp application version. It will panic if called
// more than once on a running baseapp.
func (app *BaseApp) LoadVersion(version int64) error {
	app.logger.Info("NOTICE: this could take a long time to migrate IAVL store to fastnode if you enable Fast Node.\n")
	err := app.cms.LoadVersion(version)
	if err != nil {
		return fmt.Errorf("failed to load version %d: %w", version, err)
	}

	return app.Init()
}

// LastCommitID returns the last CommitID of the multistore.
func (app *BaseApp) LastCommitID() storetypes.CommitID {
	return app.cms.LastCommitID()
}

// LastBlockHeight returns the last committed block height.
func (app *BaseApp) LastBlockHeight() int64 {
	return app.cms.LastCommitID().Version
}

// ChainID returns the chainID of the app.
func (app *BaseApp) ChainID() string {
	return app.chainID
}

// AnteHandler returns the AnteHandler of the app.
func (app *BaseApp) AnteHandler() sdk.AnteHandler {
	return app.anteHandler
}

// ConcurrencyWorkers return concurrencyWorkers of the app
func (app *BaseApp) ConcurrencyWorkers() int {
	return app.concurrencyWorkers
}

// Init initializes the app. It seals the app, preventing any
// further modifications. In addition, it validates the app against
// the earlier provided settings. Returns an error if validation fails.
// nil otherwise. Panics if the app is already sealed.
func (app *BaseApp) Init() error {
	if app.sealed {
		panic("cannot call initFromMainStore: baseapp already sealed")
	}

	emptyHeader := cmtproto.Header{ChainID: app.chainID}

	// needed for the export command which inits from store but never calls initchain
	app.setState(execModeCheck, emptyHeader)
	app.Seal()

	if app.cms == nil {
		return errors.New("commit multi-store must not be nil")
	}

	return app.cms.GetPruning().Validate()
}

func (app *BaseApp) setMinGasPrices(gasPrices sdk.DecCoins) {
	app.minGasPrices = gasPrices
}

func (app *BaseApp) setHaltHeight(haltHeight uint64) {
	app.haltHeight = haltHeight
}

func (app *BaseApp) setHaltTime(haltTime uint64) {
	app.haltTime = haltTime
}

func (app *BaseApp) setMinRetainBlocks(minRetainBlocks uint64) {
	app.minRetainBlocks = minRetainBlocks
}

func (app *BaseApp) setInterBlockCache(cache storetypes.MultiStorePersistentCache) {
	app.interBlockCache = cache
}

func (app *BaseApp) setTrace(trace bool) {
	app.trace = trace
}

func (app *BaseApp) setIndexEvents(ie []string) {
	app.indexEvents = make(map[string]struct{})

	for _, e := range ie {
		app.indexEvents[e] = struct{}{}
	}
}

// Seal seals a BaseApp. It prohibits any further modifications to a BaseApp.
func (app *BaseApp) Seal() { app.sealed = true }

// IsSealed returns true if the BaseApp is sealed and false otherwise.
func (app *BaseApp) IsSealed() bool { return app.sealed }

// setState sets the BaseApp's state for the corresponding mode with a branched
// multi-store (i.e. a CacheMultiStore) and a new Context with the same
// multi-store branch, and provided header.
func (app *BaseApp) setState(mode execMode, h cmtproto.Header) {
	ms := app.cms.CacheMultiStore()
	headerInfo := header.Info{
		Height:  h.Height,
		Time:    h.Time,
		ChainID: h.ChainID,
		AppHash: h.AppHash,
	}
	baseState := &state{
		ms: ms,
		ctx: sdk.NewContext(ms, h, false, app.logger).
			WithStreamingManager(app.streamingManager).
			WithHeaderInfo(headerInfo),
	}

	switch mode {
	case execModeCheck:
		baseState.SetContext(baseState.Context().WithIsCheckTx(true).WithMinGasPrices(app.minGasPrices))
		app.checkState = baseState

	case execModePrepareProposal:
		app.prepareProposalState = baseState

	case execModeProcessProposal:
		app.processProposalState = baseState

	case execModeFinalize:
		app.finalizeBlockState = baseState

	default:
		panic(fmt.Sprintf("invalid runTxMode for setState: %d", mode))
	}
}

// SetCircuitBreaker sets the circuit breaker for the BaseApp.
// The circuit breaker is checked on every message execution to verify if a transaction should be executed or not.
func (app *BaseApp) SetCircuitBreaker(cb CircuitBreaker) {
	if app.msgServiceRouter == nil {
		panic("cannot set circuit breaker with no msg service router set")
	}
	app.msgServiceRouter.SetCircuit(cb)
}

// GetConsensusParams returns the current consensus parameters from the BaseApp's
// ParamStore. If the BaseApp has no ParamStore defined, nil is returned.
func (app *BaseApp) GetConsensusParams(ctx sdk.Context) cmtproto.ConsensusParams {
	if app.paramStore == nil {
		return cmtproto.ConsensusParams{}
	}

	baseNamespaceCtx := ctx.WithNamepace(0)
	cp, err := app.paramStore.Get(baseNamespaceCtx)
	if err != nil {
		// This could happen while migrating from v0.45/v0.46 to v0.50, we should
		// allow it to happen so during preblock the upgrade plan can be executed
		// and the consensus params set for the first time in the new format.
		app.logger.Error("failed to get consensus params", "err", err)
		return cmtproto.ConsensusParams{}
	}

	return cp
}

// StoreConsensusParams sets the consensus parameters to the BaseApp's param
// store.
//
// NOTE: We're explicitly not storing the CometBFT app_version in the param store.
// It's stored instead in the x/upgrade store, with its own bump logic.
func (app *BaseApp) StoreConsensusParams(ctx sdk.Context, cp cmtproto.ConsensusParams) error {
	if app.paramStore == nil {
		return errors.New("cannot store consensus params with no params store set")
	}

	return app.paramStore.Set(ctx, cp)
}

// AddRunTxRecoveryHandler adds custom app.runTx method panic handlers.
func (app *BaseApp) AddRunTxRecoveryHandler(handlers ...RecoveryHandler) {
	for _, h := range handlers {
		app.runTxRecoveryMiddleware = newRecoveryMiddleware(h, app.runTxRecoveryMiddleware)
	}
}

// GetMaximumBlockGas gets the maximum gas from the consensus params. It panics
// if maximum block gas is less than negative one and returns zero if negative
// one.
func (app *BaseApp) GetMaximumBlockGas(ctx sdk.Context) uint64 {
	cp := app.GetConsensusParams(ctx)
	if cp.Block == nil {
		return 0
	}

	maxGas := cp.Block.MaxGas

	switch {
	case maxGas < -1:
		panic(fmt.Sprintf("invalid maximum block gas: %d", maxGas))

	case maxGas == -1:
		return 0

	default:
		return uint64(maxGas)
	}
}

func (app *BaseApp) validateFinalizeBlockHeight(req *abci.RequestFinalizeBlock) error {
	if req.Height < 1 {
		return fmt.Errorf("invalid height: %d", req.Height)
	}

	lastBlockHeight := app.LastBlockHeight()

	// expectedHeight holds the expected height to validate
	var expectedHeight int64
	if lastBlockHeight == 0 && app.initialHeight > 1 {
		// In this case, we're validating the first block of the chain, i.e no
		// previous commit. The height we're expecting is the initial height.
		expectedHeight = app.initialHeight
	} else {
		// This case can mean two things:
		//
		// - Either there was already a previous commit in the store, in which
		// case we increment the version from there.
		// - Or there was no previous commit, in which case we start at version 1.
		expectedHeight = lastBlockHeight + 1
	}

	if req.Height != expectedHeight {
		return fmt.Errorf("invalid height: %d; expected: %d", req.Height, expectedHeight)
	}

	return nil
}

// validateBasicTxMsgs executes basic validator calls for messages.
func validateBasicTxMsgs(msgs []sdk.Msg) error {
	if len(msgs) == 0 {
		return errorsmod.Wrap(sdkerrors.ErrInvalidRequest, "must contain at least one message")
	}

	for _, msg := range msgs {
		m, ok := msg.(sdk.HasValidateBasic)
		if !ok {
			continue
		}

		if err := m.ValidateBasic(); err != nil {
			return err
		}
	}

	return nil
}

func (app *BaseApp) getState(mode execMode) *state {
	switch mode {
	case execModeFinalize:
		return app.finalizeBlockState

	case execModePrepareProposal:
		return app.prepareProposalState

	case execModeProcessProposal:
		return app.processProposalState

	default:
		return app.checkState
	}
}

func (app *BaseApp) getBlockGasMeter(ctx sdk.Context) storetypes.GasMeter {
	if app.disableBlockGasMeter {
		return noopGasMeter{}
	}

	if maxGas := app.GetMaximumBlockGas(ctx); maxGas > 0 {
		return storetypes.NewGasMeter(maxGas)
	}

	return storetypes.NewInfiniteGasMeter()
}

func (app *BaseApp) getContext(mode execMode) sdk.Context {
	modeState := app.getState(mode)
	if modeState == nil {
		panic(fmt.Sprintf("state is nil for mode %v", mode))
	}
	ctx := modeState.Context()

	return ctx
}

// retrieve the context for the tx w/ txBytes and other memoized values.
func (app *BaseApp) getContextForTx(mode execMode, txBytes []byte) sdk.Context {
	modeState := app.getState(mode)
	if modeState == nil {
		panic(fmt.Sprintf("state is nil for mode %v", mode))
	}
	ctx := modeState.Context().
		WithTxBytes(txBytes)
	// WithVoteInfos(app.voteInfos) // TODO: identify if this is needed

	ctx = ctx.WithConsensusParams(app.GetConsensusParams(ctx))

	if mode == execModeReCheck {
		ctx = ctx.WithIsReCheckTx(true)
	}

	if mode == execModeSimulate {
		ctx, _ = ctx.CacheContext()
	}

	return ctx
}

// cacheTxContext returns a new context based off of the provided context with
// a branched multi-store.
func (app *BaseApp) cacheTxContext(ctx sdk.Context, txBytes []byte) (sdk.Context, storetypes.CacheMultiStore) {
	ms := ctx.MultiStore()
	// TODO: https://github.com/cosmos/cosmos-sdk/issues/2824
	msCache := ms.CacheMultiStore()
	if msCache.TracingEnabled() {
		msCache = msCache.SetTracingContext(
			storetypes.TraceContext(
				map[string]interface{}{
					"txHash": fmt.Sprintf("%X", tmhash.Sum(txBytes)),
				},
			),
		).(storetypes.CacheMultiStore)
	}

	return ctx.WithMultiStore(msCache), msCache
}

func (app *BaseApp) preBlock(req *abci.RequestFinalizeBlock) error {
	if app.preBlocker != nil {
		ctx := app.finalizeBlockState.Context()
		rsp, err := app.preBlocker(ctx, req)
		if err != nil {
			return err
		}
		// rsp.ConsensusParamsChanged is true from preBlocker means ConsensusParams in store get changed
		// write the consensus parameters in store to context
		if rsp.ConsensusParamsChanged {
			ctx = ctx.WithConsensusParams(app.GetConsensusParams(ctx))
			// GasMeter must be set after we get a context with updated consensus params.
			gasMeter := app.getBlockGasMeter(ctx)
			ctx = ctx.WithBlockGasMeter(gasMeter)
			app.finalizeBlockState.SetContext(ctx)
		}
	}
	return nil
}

func (app *BaseApp) beginBlock(req *abci.RequestFinalizeBlock) (sdk.BeginBlock, error) {
	var (
		resp sdk.BeginBlock
		err  error
	)

	if app.beginBlocker != nil {
		resp, err = app.beginBlocker(app.finalizeBlockState.Context())
		if err != nil {
			return resp, err
		}

		// append BeginBlock attributes to all events in the EndBlock response
		for i, event := range resp.Events {
			resp.Events[i].Attributes = append(
				event.Attributes,
				abci.EventAttribute{Key: "mode", Value: "BeginBlock"},
			)
		}

		resp.Events = sdk.MarkEventsToIndex(resp.Events, app.indexEvents)
	}

	return resp, nil
}

func (app *BaseApp) DetectInternalEvent(txs []sdk.Tx) (sdk.InternalEvents, error) {
	var (
		resp sdk.InternalEvents
		err  error
	)

	if app.internalEventDetector != nil && app.finalizeBlockState != nil {
		resp, err = app.internalEventDetector(app.finalizeBlockState.Context(), txs)
		if err != nil {
			return resp, err
		}

		// append BeginBlock attributes to all events in the EndBlock response
		for i, event := range resp.Events {
			resp.Events[i].Attributes = append(
				event.Attributes,
				abci.EventAttribute{Key: "mode", Value: "DetectInternalEvent"},
			)
		}

		resp.Events = sdk.MarkEventsToIndex(resp.Events, app.indexEvents)
	}

	return resp, nil
}

func (app *BaseApp) deliverTx(tx []byte, opts ...sdk.Context) *abci.ExecTxResult {
	t1 := time.Now()
	var ctx sdk.Context

	// Check if a context was provided
	if len(opts) > 0 {
		ctx = opts[0]
		ctx = ctx.WithTxBytes(tx).WithConsensusParams(app.GetConsensusParams(ctx))
	} else {
		ctx = app.getContextForTx(execModeFinalize, tx)
	}
	gInfo := sdk.GasInfo{}
	resultStr := "successful"

	var resp *abci.ExecTxResult

	defer func() {
		telemetry.IncrCounter(1, "tx", "count")
		telemetry.IncrCounter(1, "tx", resultStr)
		telemetry.SetGauge(float32(gInfo.GasUsed), "tx", "gas", "used")
		telemetry.SetGauge(float32(gInfo.GasWanted), "tx", "gas", "wanted")
	}()
	t2 := time.Now()

	gInfo, result, anteEvents, _, err := app.runTx(execModeFinalize, tx, ctx)
	if err != nil {
		resultStr = "failed"
		resp = sdkerrors.ResponseExecTxResultWithEvents(
			err,
			gInfo.GasWanted,
			gInfo.GasUsed,
			sdk.MarkEventsToIndex(anteEvents, app.indexEvents),
			app.trace,
		)
		return resp

	}
	t3 := time.Now()

	resp = &abci.ExecTxResult{
		GasWanted: int64(gInfo.GasWanted),
		GasUsed:   int64(gInfo.GasUsed),
		Log:       result.Log,
		Data:      result.Data,
		Events:    sdk.MarkEventsToIndex(result.Events, app.indexEvents),
	}

	t4 := time.Now()

	sdkTx, err := app.txDecoder(tx)

	t5 := time.Now()

	for _, hook := range app.deliverTxHooks {
		hook(ctx, sdkTx, sha256.Sum256(tx), resp)
	}

	fmt.Sprintf("deliverTx:", "pre runTx", t2.Sub(t1).String(), "runTx", t3.Sub(t2).String(), "build resp", t4.Sub(t3).String(), "decode", t5.Sub(t4).String(), "hook", time.Since(t5).String())

	return resp
}

// prepareNamespaceBlock is an application-defined function that is called after transactions
// have been processed in FinalizeBlock.
func (app *BaseApp) prepareNamespaceBlock(ctx context.Context, txs [][]byte, nsTable []*cmtproto.NamespaceTableEntries) error {

	if app.namespaceBlockPreparer != nil {
		namespaceMakeBLockContext := app.finalizeBlockState.Context()
		namespacesBLockData, err := app.namespaceBlockPreparer(namespaceMakeBLockContext, txs, nsTable)
		if err != nil {
			return err
		}

		for storeKey, datum := range namespacesBLockData {
			if len(datum) == 0 {
				continue
			}
			var namespaceBLock nsblockstore.NamespaceBLock
			err = borsh.Deserialize(&namespaceBLock, datum)
			if err != nil {
				return err
			}
			// todo, get namespaceId height and block hash
			namespaceBlockStore, ok := app.namespaceStoreManager.GetNamespaceStoreByKey(storeKey)
			if !ok {
				panic("Load namespace block store failed")
			}
			namespaceBlockStore.CacheBlock(&namespaceBLock)
		}

	}

	return nil
}

// makeNamespaceBlock is an application-defined function that is called after transactions
// have been processed in FinalizeBlock.
func (app *BaseApp) makeNamespaceBlock(ctx context.Context, globalEvents *[]abci.Event) error {

	if app.namespaceBlockMaker != nil {
		namespaceMakeBLockContext := app.finalizeBlockState.Context()
		namespacesBLockData, err := app.namespaceBlockMaker(namespaceMakeBLockContext, globalEvents)
		if err != nil {
			return err
		}

		for storeKey, datum := range namespacesBLockData {
			if len(datum) == 0 {
				continue
			}
			var namespaceBLock nsblockstore.NamespaceBLock
			err = borsh.Deserialize(&namespaceBLock, datum)
			if err != nil {
				return err
			}
			// todo, get namespaceId height and block hash
			namespaceBlockStore, ok := app.namespaceStoreManager.GetNamespaceStoreByKey(storeKey)
			if !ok {
				panic("Load namespace block store failed")
			}
			namespaceBlockStore.UpdateBlock(&namespaceBLock)
		}

	}

	return nil
}

// endBlock is an application-defined function that is called after transactions
// have been processed in FinalizeBlock.
func (app *BaseApp) endBlock(ctx context.Context) (sdk.EndBlock, error) {
	var endblock sdk.EndBlock

	if app.endBlocker != nil {
		endblockContext := app.finalizeBlockState.Context()
		// todo check, every moudle namespace use default 0 is ok?(include vm moudle, like evm、wasm)
		endblockContext.SetNamespace(0)
		eb, err := app.endBlocker(endblockContext)
		if err != nil {
			return endblock, err
		}

		// append EndBlock attributes to all events in the EndBlock response
		for i, event := range eb.Events {
			eb.Events[i].Attributes = append(
				event.Attributes,
				abci.EventAttribute{Key: "mode", Value: "EndBlock"},
			)
		}

		eb.Events = sdk.MarkEventsToIndex(eb.Events, app.indexEvents)
		endblock = eb
	}

	return endblock, nil
}

// runTx processes a transaction within a given execution mode, encoded transaction
// bytes, and the decoded transaction itself. All state transitions occur through
// a cached Context depending on the mode provided. State only gets persisted
// if all messages get executed successfully and the execution mode is DeliverTx.
// Note, gas execution info is always returned. A reference to a Result is
// returned if the tx does not run out of gas and if all the messages are valid
// and execute successfully. An error is returned otherwise.
func (app *BaseApp) runTx(mode execMode, txBytes []byte, opts ...sdk.Context) (gInfo sdk.GasInfo, result *sdk.Result, anteEvents []abci.Event, packetMsg *sdk.PIBCPacket, err error) {
	// NOTE: GasWanted should be returned by the AnteHandler. GasUsed is
	// determined by the GasMeter. We need access to the context to get the gas
	// meter, so we initialize upfront.
	t1 := time.Now()
	var gasWanted uint64

	var ctx sdk.Context
	var tx sdk.Tx

	// Check if a context was provided
	if len(opts) > 0 {
		ctx = opts[0]
	} else {
		ctx = app.getContextForTx(mode, txBytes)
	}
	ctx = ctx.WithNamespaceBlockManager(app.namespaceStoreManager)

	ms := ctx.MultiStore()

	// only run the tx if there is block gas remaining
	if mode == execModeFinalize && ctx.BlockGasMeter().IsOutOfGas() {
		return gInfo, nil, nil, nil, errorsmod.Wrap(sdkerrors.ErrOutOfGas, "no block gas left to run tx")
	}

	defer func() {
		if r := recover(); r != nil {
			recoveryMW := newOutOfGasRecoveryMiddleware(gasWanted, ctx, app.runTxRecoveryMiddleware)
			recoveryMW = newOCCAbortRecoveryMiddleware(recoveryMW)
			err, result = processRecovery(r, recoveryMW), nil
			ctx.Logger().Warn("panic recovered in runTx", "err", err, "height", ctx.BlockHeight(), "txIndex", ctx.TxIndex())

		}
		if err != nil {
			if !errors.Is(err, sdkerrors.ErrInvalidSequence) {
				switch m := app.mempool.(type) {
				case *mempool.NamespaceNoncer:
					m.Rollback(ctx, tx, app.getAccountNonceHandler, err)
				}
			}
		}
		gInfo = sdk.GasInfo{GasWanted: gasWanted, GasUsed: ctx.GasMeter().GasConsumed()}
	}()

	//blockGasConsumed := false

	// consumeBlockGas makes sure block gas is consumed at most once. It must
	// happen after tx processing, and must be executed even if tx processing
	// fails. Hence, it's execution is deferred.
	//consumeBlockGas := func() {
	//	//if !blockGasConsumed {
	//	//	blockGasConsumed = true
	//	//	txHash := tmbytes.HexBytes(tmtypes.Tx(txBytes).Hash())
	//	//
	//	//	before := ctx.BlockGasMeter().GasConsumedToLimit()
	//	//	ctx.BlockGasMeter().ConsumeGas(
	//	//		ctx.GasMeter().GasConsumedToLimit(), "block gas meter",
	//	//	)
	//	//	after := ctx.BlockGasMeter().GasConsumedToLimit()
	//	//	println("height=", ctx.BlockHeight(), "txHash=", fmt.Sprintf("%x", txHash), "After GasUsed=", after, "Before GasUsed=", before)
	//	//
	//	//}
	//}

	// If BlockGasMeter() panics it will be caught by the above recover and will
	// return an error - in any case BlockGasMeter will consume gas past the limit.
	//
	// NOTE: consumeBlockGas must exist in a separate defer function from the
	// general deferred recovery function to recover from consumeBlockGas as it'll
	// be executed first (deferred statements are executed as stack).
	//if mode == execModeFinalize {
	//	defer consumeBlockGas()
	//}

	t2 := time.Now()
	tx, err = app.txDecoder(txBytes)
	if err != nil {
		return sdk.GasInfo{}, nil, nil, nil, err
	}
	t3 := time.Now()
	msgs := tx.GetMsgs()
	if err := validateBasicTxMsgs(msgs); err != nil {
		return sdk.GasInfo{}, nil, nil, nil, err
	}

	for _, msg := range msgs {
		handler := app.msgServiceRouter.Handler(msg)
		if handler == nil {
			return sdk.GasInfo{}, nil, nil, nil, errorsmod.Wrapf(sdkerrors.ErrUnknownRequest, "no message handler found for %T", msg)
		}
	}
	t4 := time.Now()

	if app.anteHandler != nil {
		a1 := time.Now()
		var (
			anteCtx sdk.Context
			msCache storetypes.CacheMultiStore
		)
		if m, ok := app.mempool.(*mempool.NamespaceNoncer); ok {

			maxNonce, err := m.GetNonce(ctx, tx, app.getAccountNonceHandler)
			if err != nil {
				return sdk.GasInfo{}, nil, nil, nil, err
			}
			ctx = ctx.WithMaxNonce(maxNonce)
		}

		a2 := time.Now()
		//now := cmttime.Now()
		// Branch context before AnteHandler call in case it aborts.
		// This is required for both CheckTx and DeliverTx.
		// Ref: https://github.com/cosmos/cosmos-sdk/issues/2772
		//
		// NOTE: Alternatively, we could require that AnteHandler ensures that
		// writes do not happen if aborted/failed.  This may have some
		// performance benefits, but it'll be more difficult to get right.
		anteCtx, msCache = app.cacheTxContext(ctx, txBytes)
		anteCtx = anteCtx.WithEventManager(sdk.NewEventManager())
		newCtx, err := app.anteHandler(anteCtx, tx, mode == execModeSimulate)
		//if mode == execModeFinalize {
		//	sleepDuration := cmttime.Now().Sub(now)
		//	println("stage1 task: ", ctx.TxIndex(), " time: ", sleepDuration, " from: ", now.String())
		//}
		a3 := time.Now()
		if !newCtx.IsZero() {
			// At this point, newCtx.MultiStore() is a store branch, or something else
			// replaced by the AnteHandler. We want the original multistore.
			//
			// Also, in the case of the tx aborting, we need to track gas consumed via
			// the instantiated gas meter in the AnteHandler, so we update the context
			// prior to returning.
			ctx = newCtx.WithMultiStore(ms).WithMaxNonce(newCtx.MaxNonce())
		}

		events := ctx.EventManager().Events()

		// GasMeter expected to be set in AnteHandler
		gasWanted = ctx.GasMeter().Limit()

		if err != nil {
			return gInfo, nil, nil, nil, err
		}
		a4 := time.Now()
		//now = cmttime.Now()
		msCache.Write()
		anteEvents = events.ToABCIEvents()
		//if mode == execModeFinalize {
		//	sleepDuration := cmttime.Now().Sub(now)
		//	println("stage2 task: ", ctx.TxIndex(), " time: ", sleepDuration, " from: ", now.String())
		//}
		a5 := time.Now()
		if mode == execModeFinalize {
			fmt.Sprintf("AnteHandler",
				"getnonce", a2.Sub(a1).String(),
				"ante", a3.Sub(a2).String(),
				"gas wanted", a4.Sub(a3).String(),
				"abci event", a5.Sub(a4).String(),
			)
		}
	}

	t5 := time.Now()

	//now := cmttime.Now()
	if mode == execModeCheck {
		err = app.mempool.Insert(ctx, tx)
		switch m := app.mempool.(type) {
		case *mempool.NamespaceNoncer:
			if err == nil {
				err = m.UpdateNonce(ctx, tx, app.getAccountNonceHandler)
			}
		}
		if app.pibcPacketGeneratorHadler != nil && app.signerName != "" {
			record, err := app.signerKeyring.Key(app.signerName)
			if err != nil {
				panic(fmt.Errorf("load tx signer failed"))
			}
			signerAddr, err := record.GetAddress()
			if err != nil {
				panic(fmt.Errorf("load tx signer address failed"))
			}
			msg, err := app.pibcPacketGeneratorHadler.Handle(ctx, tx, signerAddr)
			if err != nil {
				return gInfo, nil, anteEvents, nil, err
			}
			packetMsg = msg
		}
	} else if mode == execModeFinalize {

		switch m := app.mempool.(type) {
		case *mempool.NamespaceMempool:
			err = m.RemoveWithCtx(ctx, tx)
		case *mempool.NamespaceNoncer:
			err = m.RemoveWithCtx(ctx, tx)
		default:
			err = app.mempool.Remove(tx)
		}

		if err != nil && !errors.Is(err, mempool.ErrTxNotFound) {
			return gInfo, nil, anteEvents, nil,
				fmt.Errorf("failed to remove tx from mempool: %w", err)
		}
	} else if mode == execModeReCheck {
		switch m := app.mempool.(type) {
		case *mempool.NamespaceNoncer:
			err = m.UpdateNonce(ctx, tx, app.getAccountNonceHandler)
		}
	}

	t6 := time.Now()
	//if mode == execModeFinalize {
	//	sleepDuration := cmttime.Now().Sub(now)
	//	println("stage3 task: ", ctx.TxIndex(), " time: ", sleepDuration, " from: ", now.String())
	//}
	//now = cmttime.Now()
	// Create a new Context based off of the existing Context with a MultiStore branch
	// in case message processing fails. At this point, the MultiStore
	// is a branch of a branch.
	runMsgCtx, msCache := app.cacheTxContext(ctx, txBytes)

	// Attempt to execute all messages and only update state if all messages pass
	// and we're in DeliverTx. Note, runMsgs will never return a reference to a
	// Result if any single message fails or does not have a registered Handler.
	msgsV2, err := tx.GetMsgsV2()
	if err == nil {
		result, err = app.runMsgs(runMsgCtx, msgs, msgsV2, mode)
	}
	t7 := time.Now()
	//if mode == execModeFinalize {
	//	sleepDuration := cmttime.Now().Sub(now)
	//	println("stage4 task: ", ctx.TxIndex(), " time: ", sleepDuration, " from: ", now.String())
	//}
	// Run optional postHandlers (should run regardless of the execution result).
	//
	// Note: If the postHandler fails, we also revert the runMsgs state.
	if app.postHandler != nil {
		// The runMsgCtx context currently contains events emitted by the ante handler.
		// We clear this to correctly order events without duplicates.
		// Note that the state is still preserved.
		postCtx := runMsgCtx.WithEventManager(sdk.NewEventManager())

		newCtx, errPostHandler := app.postHandler(postCtx, tx, mode == execModeSimulate, err == nil)
		if errPostHandler != nil {
			return gInfo, nil, anteEvents, nil, errors.Join(err, errPostHandler)
		}

		// we don't want runTx to panic if runMsgs has failed earlier
		if result == nil {
			result = &sdk.Result{}
		}
		result.Events = append(result.Events, newCtx.EventManager().ABCIEvents()...)
	}
	t8 := time.Now()
	//now = cmttime.Now()
	if err == nil {
		if mode == execModeFinalize {
			// When block gas exceeds, it'll panic and won't commit the cached store.
			//consumeBlockGas()

			msCache.Write()
		}

		if len(anteEvents) > 0 && (mode == execModeFinalize || mode == execModeSimulate) {
			// append the events in the order of occurrence
			result.Events = append(anteEvents, result.Events...)
		}
	}
	t9 := time.Now()
	if mode == execModeFinalize {
		//sleepDuration := cmttime.Now().Sub(now)
		//println("stage5 task: ", ctx.TxIndex(), " time: ", sleepDuration, " from: ", now.String())
		fmt.Sprintf("runMsg",
			"Prepare Ctx", t2.Sub(t1).String(),
			"decode Tx", t3.Sub(t2).String(),
			"msg touter", t4.Sub(t3).String(),
			"ante handler", t5.Sub(t4).String(),
			"mempool rm tx", t6.Sub(t5).String(),
			"run msg", t7.Sub(t6).String(),
			"post handler", t8.Sub(t7).String(),
			"cache write", t9.Sub(t8).String(),
		)
	}
	return gInfo, result, anteEvents, packetMsg, err
}

// runMsgs iterates through a list of messages and executes them with the provided
// Context and execution mode. Messages will only be executed during simulation
// and DeliverTx. An error is returned if any single message fails or if a
// Handler does not exist for a given message route. Otherwise, a reference to a
// Result is returned. The caller must not commit state if an error is returned.
func (app *BaseApp) runMsgs(ctx sdk.Context, msgs []sdk.Msg, msgsV2 []protov2.Message, mode execMode) (*sdk.Result, error) {
	events := sdk.EmptyEvents()
	var msgResponses []*codectypes.Any

	// NOTE: GasWanted is determined by the AnteHandler and GasUsed by the GasMeter.
	for i, msg := range msgs {
		if mode != execModeFinalize && mode != execModeSimulate {
			break
		}

		handler := app.msgServiceRouter.Handler(msg)
		if handler == nil {
			return nil, errorsmod.Wrapf(sdkerrors.ErrUnknownRequest, "no message handler found for %T", msg)
		}

		// ADR 031 request type routing
		msgResult, err := handler(ctx, msg)
		if err != nil {
			return nil, errorsmod.Wrapf(err, "failed to execute message; message index: %d", i)
		}

		// create message events
		msgEvents, err := createEvents(app.cdc, msgResult.GetEvents(), msg, msgsV2[i])
		if err != nil {
			return nil, errorsmod.Wrapf(err, "failed to create message events; message index: %d", i)
		}

		// append message events and data
		//
		// Note: Each message result's data must be length-prefixed in order to
		// separate each result.
		for j, event := range msgEvents {
			// append message index to all events
			msgEvents[j] = event.AppendAttributes(sdk.NewAttribute("msg_index", strconv.Itoa(i)))
		}

		events = events.AppendEvents(msgEvents)

		// Each individual sdk.Result that went through the MsgServiceRouter
		// (which should represent 99% of the Msgs now, since everyone should
		// be using protobuf Msgs) has exactly one Msg response, set inside
		// `WrapServiceResult`. We take that Msg response, and aggregate it
		// into an array.
		if len(msgResult.MsgResponses) > 0 {
			msgResponse := msgResult.MsgResponses[0]
			if msgResponse == nil {
				return nil, sdkerrors.ErrLogic.Wrapf("got nil Msg response at index %d for msg %s", i, sdk.MsgTypeURL(msg))
			}
			msgResponses = append(msgResponses, msgResponse)
		}

	}

	data, err := makeABCIData(msgResponses)
	if err != nil {
		return nil, errorsmod.Wrap(err, "failed to marshal tx data")
	}

	return &sdk.Result{
		Data:         data,
		Events:       events.ToABCIEvents(),
		MsgResponses: msgResponses,
	}, nil
}

// makeABCIData generates the Data field to be sent to ABCI Check/DeliverTx.
func makeABCIData(msgResponses []*codectypes.Any) ([]byte, error) {
	return proto.Marshal(&sdk.TxMsgData{MsgResponses: msgResponses})
}

func createEvents(cdc codec.Codec, events sdk.Events, msg sdk.Msg, msgV2 protov2.Message) (sdk.Events, error) {
	eventMsgName := sdk.MsgTypeURL(msg)
	msgEvent := sdk.NewEvent(sdk.EventTypeMessage, sdk.NewAttribute(sdk.AttributeKeyAction, eventMsgName))

	// we set the signer attribute as the sender
	signers, err := cdc.GetMsgV2Signers(msgV2)
	if err != nil {
		return nil, err
	}
	if len(signers) > 0 && signers[0] != nil {
		addrStr, err := cdc.InterfaceRegistry().SigningContext().AddressCodec().BytesToString(signers[0])
		if err != nil {
			return nil, err
		}
		msgEvent = msgEvent.AppendAttributes(sdk.NewAttribute(sdk.AttributeKeySender, addrStr))
	}

	// verify that events have no module attribute set
	if _, found := events.GetAttributes(sdk.AttributeKeyModule); !found {
		if moduleName := sdk.GetModuleNameFromTypeURL(eventMsgName); moduleName != "" {
			msgEvent = msgEvent.AppendAttributes(sdk.NewAttribute(sdk.AttributeKeyModule, moduleName))
		}
	}

	return sdk.Events{msgEvent}.AppendEvents(events), nil
}

// sortTxs return sort txs and nsTable & orders
func (app *BaseApp) sortTxs(ctx sdk.Context, req *abci.RequestSortTxs) (*abci.ResponseSortTxs, error) {
	startTime := time.Now()
	reqByteTxs, _ := app.TxDecodeBatch(req.Txs)
	app.logger.Info("1->sortTxs TxDecodeBatch time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(req.Txs), "sortType", req.SortType, "height", ctx.BlockHeight())
	insertTime := time.Now()
	var (
		sortTxs                 = make([][]byte, 0)
		wg                      sync.WaitGroup
		nsMempool               = mempool.NewNamespaceMempool()
		beforeInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
		afterInterChainPairTxs  = make(map[string][]*cmtproto.NsInterChainTransactionIndex)

		nsMempoolInsertTxs = func(ctx sdk.Context, namespaceTxs []sdk.ByteTx, namespaceId uint64) {
			defer wg.Done()
			singleStartTime := time.Now()
			for idx, byteTx := range namespaceTxs {
				tx := byteTx.Tx
				txBz := byteTx.TxBytes
				if tx == nil || txBz == nil {
					continue
				}

				txIdBz, isCross, _ := app.crossNamespaceTransactionIdentifierHandler(ctx.WithTxBytes(txBz), tx)
				if isCross {
					txId := tmbytes.HexBytes(txIdBz).String()
					if _, ok := beforeInterChainPairTxs[txId]; ok {
						beforeInterChainPairTxs[txId] = append(beforeInterChainPairTxs[txId], &cmtproto.NsInterChainTransactionIndex{
							Namespace: namespaceId,
							Index:     int32(idx),
						})
					} else {
						beforeInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
							{
								Namespace: namespaceId,
								Index:     int32(idx),
							},
						}
					}
				}

				ctx = ctx.WithNamepace(namespaceId)
				_ = nsMempool.Insert(ctx, tx)

			}
			app.logger.Info("2.1->sortTxs single namespace insert time", "used", fmt.Sprintf("%v ms", time.Since(singleStartTime).Milliseconds()), "size", len(namespaceTxs), "namespaceId", namespaceId, "sortType", req.SortType, "height", ctx.BlockHeight())
		}
	)
	nsByteTxs := sdk.ListNamespaceByteTxs(reqByteTxs, req.NsTable)

	for namespaceId, byteTxs := range nsByteTxs {
		wg.Add(1)
		go nsMempoolInsertTxs(ctx, byteTxs, namespaceId)
	}
	wg.Wait()
	app.logger.Info("2->sortTxs total insert time", "used", fmt.Sprintf("%v ms", time.Since(insertTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "sortType", req.SortType, "height", ctx.BlockHeight())

	sortTime := time.Now()
	nsIterators := nsMempool.SelectWithNsTable(ctx, req.Txs, req.NsTable)

	app.logger.Info("3->sortTxs sort time", "used", fmt.Sprintf("%v ms", time.Since(sortTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "sortType", req.SortType, "height", ctx.BlockHeight())

	var (
		offset  = 0
		nsTable = make([]*cmtproto.NamespaceTableEntries, 0)
		orders  = &cmtproto.Orders{
			NsTransactionOrders: &cmtproto.NsTransactionOrders{
				Entries: make([]*cmtproto.NsTransactionOrderEntry, 0)},
			NsInterChainTransactionOrders: &cmtproto.NsInterChainTransactionOrders{
				Entries: make([]*cmtproto.NsInterChainTransactionOrderEntry, 0),
			},
		}
		txIndex = 0
	)
	staticTime := time.Now()
	handleNsInterChainTransactionOrders := func(byteTx sdk.ByteTx, txIdx int, namespaceId uint64) bool {
		tx := byteTx.Tx
		txBz := byteTx.TxBytes
		txIdBz, isCross, isFirst := app.crossNamespaceTransactionIdentifierHandler(ctx.WithTxBytes(txBz), tx)
		if isCross {
			txId := tmbytes.HexBytes(txIdBz).String()
			if pairTxs, inPair := beforeInterChainPairTxs[txId]; inPair {
				if len(pairTxs) != 2 {
					return false
				}
			}

			if interTxs, ok := afterInterChainPairTxs[txId]; ok {
				interTxs = append(interTxs, &cmtproto.NsInterChainTransactionIndex{
					Namespace: namespaceId,
					Index:     int32(txIdx),
				})

				idxes := make([]*cmtproto.NsInterChainTransactionIndex, 2)

				if isFirst {
					idxes[0] = interTxs[1]
					idxes[1] = interTxs[0]
				} else {
					idxes[0] = interTxs[0]
					idxes[1] = interTxs[1]
				}

				entry := &cmtproto.NsInterChainTransactionOrderEntry{
					Indexes: idxes,
				}
				orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
			} else {
				afterInterChainPairTxs[txId] = []*cmtproto.NsInterChainTransactionIndex{
					{
						Namespace: namespaceId,
						Index:     int32(txIdx),
					},
				}
			}
		}
		return true
	}

	var selectedTxsNums int
	selectedTxsSignersSeqs := make(map[string]uint64)

	isNeedCheck := req.SortType == uint32(bfttypes.SortAndCheck)

	for _, nsIterator := range nsIterators {
		iterator := nsIterator.Iterator
		namespaceId := nsIterator.NamespaceId

		txSize := nsMempool.CountTxByNamespaceId(namespaceId)
		// Static analysis
		var (
			byteTxs              = make([]sdk.ByteTx, 0, txSize)
			addressIndexMap      = make(map[string]int)
			lastTxIndexMap       = make(map[string]int)
			nsTransactionIndexes = make([]*cmtproto.NsTransactionIndexes, 0, txSize)
			baseFee              = big.NewInt(0)
		)

		// If the iterator is nil use the NoOpMempool, get all namespace txs
		if iterator == nil {
			byteTxs = nsByteTxs[namespaceId]
		} else {
			txs := make([]sdk.Tx, 0, txSize)
			for iterator != nil {
				tx := iterator.Tx()
				txs = append(txs, tx)
				iterator = iterator.Next()
			}
			batchTime := time.Now()
			byteTxs, _ = app.TxEncodeBatch(txs)
			app.logger.Info("4.1->sortTxs iterator TxEncodeBatch  time", "used", fmt.Sprintf("%v ms", time.Since(batchTime).Milliseconds()), "namespaceId", namespaceId, "sortType", req.SortType, "height", ctx.BlockHeight())
		}

		handleNsTransactionDependOrders := func(txIdx int, namespaceId uint64, from string, to string) {
			if lastToIdx, toExists := lastTxIndexMap[to]; toExists {
				if lastFromIdx, fromExists := lastTxIndexMap[from]; !fromExists || lastFromIdx != lastToIdx {
					entry := &cmtproto.NsInterChainTransactionOrderEntry{
						Indexes: []*cmtproto.NsInterChainTransactionIndex{
							{
								Namespace: namespaceId,
								Index:     int32(lastToIdx),
							},
							{
								Namespace: namespaceId,
								Index:     int32(txIdx),
							},
						},
					}
					orders.NsInterChainTransactionOrders.Entries = append(orders.NsInterChainTransactionOrders.Entries, entry)
				}
			}
			lastTxIndexMap[from] = txIdx
			lastTxIndexMap[to] = txIdx
		}

		handleNsTransactionOrders := func(from string, to string, txIdx int, namespaceId uint64) {

			var (
				fromIdx    int
				toIdx      int
				fromExists bool
				toExists   bool
				curIdx     int
				isToSerial bool
				toIsSerial = func(namespaceId uint64, to string) bool {
					isContract, isContractSerial := app.contractStaticAnalysisResultHandler(ctx.WithNamepace(namespaceId), to)
					if isContract {
						return isContractSerial
					}
					return true
				}
			)

			defer func() {
				if isToSerial {
					handleNsTransactionDependOrders(txIdx, namespaceId, from, to)
				}
			}()

			fromIdx, fromExists = addressIndexMap[from]
			toIdx, toExists = addressIndexMap[to]

			if fromExists && toExists {
				//Both exist,add from index
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))

				//Check if they should be depends
				if toIsSerial(namespaceId, to) {
					isToSerial = true
				}
			} else if fromExists {
				// Only from exists, add to index
				curIdx = fromIdx
				nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
				addressIndexMap[to] = curIdx
			} else if toExists {
				// Only to exists, add from index
				if toIsSerial(namespaceId, to) {
					isToSerial = true
					//append to index
					curIdx = toIdx
					nsTransactionIndexes[curIdx].Indexes = append(nsTransactionIndexes[curIdx].Indexes, int32(txIdx))
					addressIndexMap[from] = curIdx
				} else {
					//create new from index
					nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
					curIdx = len(nsTransactionIndexes) - 1
					addressIndexMap[from] = curIdx
				}
			} else {
				// Neither exists, create both indices
				//create new from & to index
				nsTransactionIndexes = append(nsTransactionIndexes, &cmtproto.NsTransactionIndexes{Indexes: []int32{int32(txIdx)}})
				curIdx = len(nsTransactionIndexes) - 1
				addressIndexMap[from] = curIdx
				addressIndexMap[to] = curIdx
			}
		}

		orderTime := time.Now()
		if isNeedCheck {
			baseFee = app.baseFeeHandler(ctx.WithNamepace(namespaceId))
		}

		for _, byteTx := range byteTxs {
			txBz := byteTx.TxBytes
			tx := byteTx.Tx
			if tx == nil || txBz == nil {
				continue
			}
			from, to, nonce := app.txAccountInfoHandler(tx)

			txSignersSeqs := make(map[string]uint64)
			if isNeedCheck {
				seq, ok := selectedTxsSignersSeqs[from]
				if !ok {
					txSignersSeqs[from] = nonce
				} else {
					// If we have seen this signer before in this block, we must make
					// sure that the current sequence is seq+1; otherwise is invalid
					// and we skip it.
					if seq+1 != nonce {
						continue
					}
					txSignersSeqs[from] = nonce
				}

				isFeeVerified := app.checkTxFeeHandler(ctx, baseFee, tx)
				if !isFeeVerified {
					continue
				}
			}
			isInterChainPaired := handleNsInterChainTransactionOrders(byteTx, txIndex, namespaceId)
			if !isInterChainPaired {
				continue
			}

			sortTxs = append(sortTxs, txBz)
			offset++
			handleNsTransactionOrders(from, to, txIndex, namespaceId)
			txIndex++

			if isNeedCheck {
				txsLen := len(sortTxs)
				for sender, senderSeq := range txSignersSeqs {
					// If txsLen != selectedTxsNums is true, it means that we've
					// added a new tx to the selected txs, so we need to update
					// the sequence of the sender.
					if txsLen != selectedTxsNums {
						selectedTxsSignersSeqs[sender] = senderSeq
					} else if _, ok := selectedTxsSignersSeqs[sender]; !ok {
						// The transaction hasn't been added but it passed the
						// verification, so we know that the sequence is correct.
						// So we set this sender's sequence to seq-1, in order
						// to avoid unnecessary calls to PrepareProposalVerifyTx.
						selectedTxsSignersSeqs[sender] = senderSeq - 1
					}
				}
				selectedTxsNums = txsLen
			}

		}
		app.logger.Info("4.2->sortTxs orders time", "used", fmt.Sprintf("%v ms", time.Since(orderTime).Milliseconds()), "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "sortType", req.SortType, "height", ctx.BlockHeight())

		if len(nsTransactionIndexes) > 0 {
			nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
				Namespace: namespaceId,
				Offset:    int32(offset),
			})
			orders.NsTransactionOrders.Entries = append(orders.NsTransactionOrders.Entries, &cmtproto.NsTransactionOrderEntry{
				Namespace: namespaceId,
				Indexes:   nsTransactionIndexes,
			})
		}

	}
	app.logger.Info("4->sortTxs total orders time", "used", fmt.Sprintf("%v ms", time.Since(staticTime).Milliseconds()), "size", len(sortTxs), "nsTable", nsTable, "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "sortType", req.SortType, "height", ctx.BlockHeight())
	app.logger.Info("5->sortTxs total time", "used", fmt.Sprintf("%v ms", time.Since(startTime).Milliseconds()), "size", len(sortTxs), "nsTable", nsTable, "reqSize", len(req.Txs), "reqNsTable", req.NsTable, "sortType", req.SortType, "height", ctx.BlockHeight())

	return &abci.ResponseSortTxs{Txs: sortTxs, NsTable: nsTable, Orders: orders}, nil
}

// PrepareProposalVerifyTx performs transaction verification when a proposer is
// creating a block proposal during PrepareProposal. Any state committed to the
// PrepareProposal state internally will be discarded. <nil, err> will be
// returned if the transaction cannot be encoded. <bz, nil> will be returned if
// the transaction is valid, otherwise <bz, err> will be returned.
func (app *BaseApp) PrepareProposalVerifyTx(tx sdk.Tx) ([]byte, error) {
	bz, err := app.txEncoder(tx)
	if err != nil {
		return nil, err
	}

	_, _, _, _, err = app.runTx(execModePrepareProposal, bz)
	if err != nil {
		return nil, err
	}

	return bz, nil
}

// ProcessProposalVerifyTx performs transaction verification when receiving a
// block proposal during ProcessProposal. Any state committed to the
// ProcessProposal state internally will be discarded. <nil, err> will be
// returned if the transaction cannot be decoded. <Tx, nil> will be returned if
// the transaction is valid, otherwise <Tx, err> will be returned.
func (app *BaseApp) ProcessProposalVerifyTx(txBz []byte) (sdk.Tx, error) {
	tx, err := app.txDecoder(txBz)
	if err != nil {
		return nil, err
	}

	_, _, _, _, err = app.runTx(execModeProcessProposal, txBz)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (app *BaseApp) TxDecode(txBytes []byte) (sdk.Tx, error) {
	return app.txDecoder(txBytes)
}

func (app *BaseApp) TxEncode(tx sdk.Tx) ([]byte, error) {
	return app.txEncoder(tx)
}

func (app *BaseApp) TxDecodeBatch(txsBytes [][]byte) ([]sdk.ByteTx, error) {

	wg := sync.WaitGroup{}
	decodeTxs := make([]sdk.ByteTx, len(txsBytes))

	//f, _ := os.Create("trace.out")
	//
	//if err := trace.Start(f); err != nil {
	//	//panic(err)
	//}
	for idx, txBz := range txsBytes {
		wg.Add(1)
		go func(txBz []byte, idx int) {
			defer wg.Done()
			tx, err := app.txDecoder(txBz)
			if err == nil {
				decodeTxs[idx] = sdk.ByteTx{Tx: tx, TxBytes: txBz}
			}
		}(txBz, idx)
	}
	//trace.Stop()
	wg.Wait()

	return decodeTxs, nil
}
func (app *BaseApp) TxEncodeBatch(txs []sdk.Tx) ([]sdk.ByteTx, error) {

	wg := sync.WaitGroup{}
	encodeTxs := make([]sdk.ByteTx, len(txs))
	for idx, tx := range txs {
		wg.Add(1)
		go func(tx sdk.Tx, idx int) {
			defer wg.Done()
			txBz, err := app.txEncoder(tx)
			if err == nil {
				encodeTxs[idx] = sdk.ByteTx{Tx: tx, TxBytes: txBz}
			}
		}(tx, idx)
	}
	wg.Wait()
	return encodeTxs, nil
}

// Close is called in start cmd to gracefully cleanup resources.
func (app *BaseApp) Close() error {
	var errs []error

	// Close app.db (opened by cosmos-sdk/server/start.go call to openDB)
	if app.db != nil {
		app.logger.Info("Closing application.db")
		if err := app.db.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	// Close app.snapshotManager
	// - opened when app chains use cosmos-sdk/server/util.go/DefaultBaseappOptions (boilerplate)
	// - which calls cosmos-sdk/server/util.go/GetSnapshotStore
	// - which is passed to baseapp/options.go/SetSnapshot
	// - to set app.snapshotManager = snapshots.NewManager
	if app.snapshotManager != nil {
		app.logger.Info("Closing snapshots/metadata.db")
		if err := app.snapshotManager.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func LoadNamespaceStoreInfo(db dbm.DB, home string) (*nsblockstore.NamespaceStoreDBInfo, error) {
	validNamespaces := ListDBNamespace(db)

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

func (app *BaseApp) SetNamespace(mode execMode, namespaceId uint64) {
	switch mode {
	case execModeFinalize:
		app.finalizeBlockState.SetContext(app.finalizeBlockState.Context().WithNamepace(namespaceId))

	case execModePrepareProposal:
		app.prepareProposalState.SetContext(app.prepareProposalState.Context().WithNamepace(namespaceId))

	case execModeProcessProposal:
		app.processProposalState.SetContext(app.processProposalState.Context().WithNamepace(namespaceId))

	default:
		app.checkState.SetContext(app.checkState.Context().WithNamepace(namespaceId))
	}

}

func (app *BaseApp) RegisterDeliverTxHook() {
	app.deliverTxHooks = append(app.deliverTxHooks, func(ctx sdk.Context, tx sdk.Tx, checksum [32]byte, resp *abci.ExecTxResult) {
		app.addCosmosEventsToEVMReceiptIfApplicable(ctx, tx, checksum, resp)
	})
}
