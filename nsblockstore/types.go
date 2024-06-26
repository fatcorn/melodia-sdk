package nsblockstore

import cmtdbm "github.com/cometbft/cometbft-db"
import "sync"

type NamespaceStoreManager struct {
	nsStores               map[uint64]NSBlockStore
	namespaceIdsModule     map[uint64]string
	moduleRelatedModules   map[string][]string
	dbRootDir              string
	namespaceActivateState map[uint64]bool
	lock                   sync.RWMutex
}

func NewNamespaceStoreManager() *NamespaceStoreManager {
	return &NamespaceStoreManager{
		nsStores:               make(map[uint64]NSBlockStore),
		namespaceIdsModule:     make(map[uint64]string),
		moduleRelatedModules:   make(map[string][]string),
		namespaceActivateState: make(map[uint64]bool),
	}
}

func (manager *NamespaceStoreManager) SetDBRootDir(dbRootDir string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.dbRootDir = dbRootDir
}

func (manager *NamespaceStoreManager) GetDBRootDir() string {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.dbRootDir
}

func (manager *NamespaceStoreManager) GetNamespaceStore(namespaceId uint64) (NSBlockStore, bool) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	nsBlockStore, ok := manager.nsStores[namespaceId]
	return nsBlockStore, ok
}

func (manager *NamespaceStoreManager) AddNamespaceStore(namespaceId uint64, store NSBlockStore) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.nsStores[namespaceId] = store
}

func (manager *NamespaceStoreManager) GetNamespaceIdsModule() map[uint64]string {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.namespaceIdsModule
}

func (manager *NamespaceStoreManager) AddNamespaceIdsModule(namespaceId uint64, moduleName string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.namespaceIdsModule[namespaceId] = moduleName
}

func (manager *NamespaceStoreManager) GetModuleRelatedModules() map[string][]string {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.moduleRelatedModules
}

func (manager *NamespaceStoreManager) AddModuleRelatedModules(module string, relatedModuleNames []string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.moduleRelatedModules[module] = relatedModuleNames
}

func (manager *NamespaceStoreManager) GetNamespaceActiveState() map[uint64]bool {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.namespaceActivateState
}

func (manager *NamespaceStoreManager) AddNamespaceActiveState(namespaceId uint64, isActive bool) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	manager.namespaceActivateState[namespaceId] = isActive
}

type NamespaceStoreDBInfo struct {
	NamespaceStoreDBs    map[uint64]cmtdbm.DB
	DbRootDir            string
	NamespacesModuleName map[uint64]string
	ModuleRelatedModules map[string][]string
}
