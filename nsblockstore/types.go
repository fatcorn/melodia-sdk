package nsblockstore

import cmtdbm "github.com/cometbft/cometbft-db"
import "sync"
import "fmt"
import "slices"

type NamespaceStoreManager struct {
	nsStores               map[string]NSBlockStore
	namespaceIdsModule     map[string]*NamespaceModule
	moduleRelatedModules   map[string][]string
	dbRootDir              string
	namespaceActivateState map[string]bool
	lock                   sync.RWMutex
}

type NamespaceModule struct {
	NamespaceId uint64
	ModuleName  string
}

func CaculateStoreKey(namespaceId uint64, moduleName string) string {
	return fmt.Sprintf("%v_%v", moduleName, namespaceId)
}

func NewNamespaceStoreManager() *NamespaceStoreManager {
	return &NamespaceStoreManager{
		nsStores:               make(map[string]NSBlockStore),
		namespaceIdsModule:     make(map[string]*NamespaceModule),
		moduleRelatedModules:   make(map[string][]string),
		namespaceActivateState: make(map[string]bool),
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

func (manager *NamespaceStoreManager) GetNamespaceStore(namespaceId uint64, moduleName string) (NSBlockStore, bool) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	nsBlockStore, ok := manager.nsStores[key]
	return nsBlockStore, ok
}

func (manager *NamespaceStoreManager) GetNamespaceStoreByKey(key string) (NSBlockStore, bool) {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	nsBlockStore, ok := manager.nsStores[key]
	return nsBlockStore, ok
}

func (manager *NamespaceStoreManager) AddNamespaceStore(namespaceId uint64, moduleName string, store NSBlockStore) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	manager.nsStores[key] = store
}

func (manager *NamespaceStoreManager) GetNamespaceIdsModule() map[string]*NamespaceModule {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.namespaceIdsModule
}

func (manager *NamespaceStoreManager) AddNamespaceIdsModule(namespaceId uint64, moduleName string) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	manager.namespaceIdsModule[key] = &NamespaceModule{
		NamespaceId: namespaceId,
		ModuleName:  moduleName,
	}
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

func (manager *NamespaceStoreManager) GetNamespaceActiveState() map[string]bool {
	manager.lock.RLock()
	defer manager.lock.RUnlock()
	return manager.namespaceActivateState
}

func (manager *NamespaceStoreManager) AddNamespaceActiveState(namespaceId uint64, moduleName string, isActive bool) {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	manager.namespaceActivateState[key] = isActive
}

func (manager *NamespaceStoreManager) IsMoudleStoreKeyExist(moudleName string, namespaceId uint64) bool {
	manager.lock.Lock()
	defer manager.lock.Unlock()
	for _, value := range manager.namespaceIdsModule {
		if value.NamespaceId == namespaceId {
			for key, relatedMoudles := range manager.moduleRelatedModules {
				if key == moudleName || slices.Contains(relatedMoudles, moudleName) {
					return true
				}
			}
		}
	}
	return false
}

type NamespaceStoreDBInfo struct {
	NamespaceStoreDBs    map[string]cmtdbm.DB
	DbRootDir            string
	NamespacesModuleName map[string]NamespaceModule
	ModuleRelatedModules map[string][]string
}
