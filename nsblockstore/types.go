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
	sync.RWMutex
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
	manager.Lock()
	defer manager.Unlock()
	manager.dbRootDir = dbRootDir
}

func (manager *NamespaceStoreManager) GetDBRootDir() string {
	manager.RLock()
	defer manager.RUnlock()
	return manager.dbRootDir
}

func (manager *NamespaceStoreManager) GetNamespaceStore(namespaceId uint64, moduleName string) (NSBlockStore, bool) {
	manager.RLock()
	defer manager.RUnlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	nsBlockStore, ok := manager.nsStores[key]
	return nsBlockStore, ok
}

func (manager *NamespaceStoreManager) GetNamespaceStoreByKey(key string) (NSBlockStore, bool) {
	manager.RLock()
	defer manager.RUnlock()
	nsBlockStore, ok := manager.nsStores[key]
	return nsBlockStore, ok
}

func (manager *NamespaceStoreManager) AddNamespaceStore(namespaceId uint64, moduleName string, store NSBlockStore) {
	manager.Lock()
	defer manager.Unlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	manager.nsStores[key] = store
}

func (manager *NamespaceStoreManager) GetNamespaceIdsModule() map[string]*NamespaceModule {
	manager.RLock()
	defer manager.RUnlock()
	return manager.namespaceIdsModule
}

func (manager *NamespaceStoreManager) AddNamespaceIdsModule(namespaceId uint64, moduleName string) {
	manager.Lock()
	defer manager.Unlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	manager.namespaceIdsModule[key] = &NamespaceModule{
		NamespaceId: namespaceId,
		ModuleName:  moduleName,
	}
}

func (manager *NamespaceStoreManager) GetModuleRelatedModules() map[string][]string {
	manager.RLock()
	defer manager.RUnlock()
	return manager.moduleRelatedModules
}

func (manager *NamespaceStoreManager) AddModuleRelatedModules(module string, relatedModuleNames []string) {
	manager.Lock()
	defer manager.Unlock()
	manager.moduleRelatedModules[module] = relatedModuleNames
}

func (manager *NamespaceStoreManager) GetNamespaceActiveState() map[string]bool {
	manager.RLock()
	defer manager.RUnlock()
	return manager.namespaceActivateState
}

func (manager *NamespaceStoreManager) AddNamespaceActiveState(namespaceId uint64, moduleName string, isActive bool) {
	manager.Lock()
	defer manager.Unlock()
	key := CaculateStoreKey(namespaceId, moduleName)
	manager.namespaceActivateState[key] = isActive
}

func (manager *NamespaceStoreManager) IsMoudleStoreKeyExist(moudleName string, namespaceId uint64) bool {
	manager.Lock()
	defer manager.Unlock()
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

func (manager *NamespaceStoreManager) Commit() {
	manager.RLock()
	defer manager.RUnlock()
	wg := sync.WaitGroup{}
	for _, store := range manager.nsStores {
		wg.Add(1)
		go func(blockStore NSBlockStore) {
			defer wg.Done()
			blockStore.Commit()
		}(store)
	}
	wg.Wait()
}

type NamespaceStoreDBInfo struct {
	NamespaceStoreDBs    map[string]cmtdbm.DB
	DbRootDir            string
	NamespacesModuleName map[string]NamespaceModule
	ModuleRelatedModules map[string][]string
}
