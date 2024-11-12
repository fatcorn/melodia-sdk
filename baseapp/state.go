package baseapp

import (
	"sync"

	storetypes "cosmossdk.io/store/types"

	sdk "github.com/cosmos/cosmos-sdk/types"
)

type state struct {
	ms storetypes.CacheMultiStore

	mtx sync.RWMutex
	ctx sdk.Context
}

// CacheMultiStore calls and returns a CacheMultiStore on the state's underling
// CacheMultiStore.
func (st *state) CacheMultiStore() storetypes.CacheMultiStore {
	return st.ms.CacheMultiStore()
}

// SetContext updates the state's context to the context provided.
func (st *state) SetContext(ctx sdk.Context) {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	st.ctx = ctx
}

// SetContext updates the state's context to the context provided.
func (st *state) SetCache(ms storetypes.CacheMultiStore) {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	st.ms = ms
}

// SetContext updates the state's context to the context provided.
func (st *state) GetCache() storetypes.CacheMultiStore {
	st.mtx.Lock()
	defer st.mtx.Unlock()
	return st.ms
}

// Context returns the Context of the state.
func (st *state) Context() sdk.Context {
	st.mtx.RLock()
	defer st.mtx.RUnlock()
	return st.ctx
}
