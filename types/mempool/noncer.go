// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package mempool

import (
	"cosmossdk.io/store/multiversion"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"sync"
)

// noncer is a tiny virtual state database to manage the executable nonces of
// accounts in the pool, falling back to reading from a real state database if
// an account is unknown.
type noncer struct {
	fallback sdk.GetAccountNonceHandler
	nonces   *multiversion.ShardedMap
}

// newNoncer creates a new virtual state database to track the pool nonces.
func newNoncer(fallback sdk.GetAccountNonceHandler) *noncer {
	return &noncer{
		fallback: fallback,
		nonces:   multiversion.NewShardedMap(20000),
	}
}

// get returns the current nonce of an account, falling back to a real state
// database if the account is unknown.
func (txn *noncer) get(ctx sdk.Context, addr string) uint64 {
	// We use mutex for get operation is the underlying
	// state will mutate db even for read access.
	if _, ok := txn.nonces.Get(addr); !ok {
		nonce, err := txn.fallback(ctx, addr)
		if err != nil {
			panic(err)
		}
		if nonce != 0 {
			txn.nonces.Set(addr, nonce)
		}
	}
	nonce, ok := txn.nonces.Get(addr)
	if ok {
		return nonce.(uint64)
	}
	return 0
}

func (txn *noncer) refresh(ctx sdk.Context, addr string) uint64 {
	// We use mutex for get operation is the underlying
	// state will mutate db even for read access.
	nonce, err := txn.fallback(ctx, addr)
	if err != nil {
		panic(err)
	}
	if nonce != 0 {
		txn.nonces.Set(addr, nonce)
	}
	return nonce
}

// set inserts a new virtual nonce into the virtual state database to be returned
// whenever the pool requests it instead of reaching into the real state database.
func (txn *noncer) set(_ sdk.Context, addr string, nonce uint64) {
	txn.nonces.Set(addr, nonce)
}

// setIfLower updates a new virtual nonce into the virtual state database if the
// new one is lower.
func (txn *noncer) setIfLower(addr string, nonce uint64) {
	txn.nonces.SetIf(addr, nonce, func(oldNonce interface{}, exists bool) bool {
		if exists && oldNonce.(uint64) <= nonce {
			return false
		}
		return true
	})
}
func (txn *noncer) setIfHigher(addr string, nonce uint64) uint64 {
	nextNonce := txn.nonces.SetIf(addr, nonce, func(oldNonce interface{}, exists bool) bool {
		if exists && oldNonce.(uint64) > nonce {
			return false
		}
		return true
	})

	return nextNonce.(uint64)
}

// Reset reset the nonces
func (txn *noncer) Reset() {
	txn.nonces = multiversion.NewShardedMap(20000)
}

type namepaceNoncer struct {
	noncers *sync.Map
	mux     sync.Mutex
}

func newNamepsaceNoncer() *namepaceNoncer {
	return &namepaceNoncer{
		noncers: &sync.Map{},
	}
}

func (ns *namepaceNoncer) getNonce(ctx sdk.Context, sender string) uint64 {
	finalNoncer := ns.getNoncer(ctx, func(ctx sdk.Context, sender string) (uint64, error) {
		return 0, nil
	})
	return finalNoncer.get(ctx, sender)
}

func (ns *namepaceNoncer) updateNonce(ctx sdk.Context, sender string, nonce uint64) uint64 {
	finalNoncer := ns.getNoncer(ctx, func(ctx sdk.Context, sender string) (uint64, error) {
		return 0, nil
	})
	return finalNoncer.setIfHigher(sender, nonce)
}

func (ns *namepaceNoncer) getNoncer(ctx sdk.Context, fallback sdk.GetAccountNonceHandler) *noncer {
	namespaceId := ctx.Namespace()
	finalNoncer, init := ns.noncers.Load(namespaceId)
	if !init {
		ns.mux.Lock()
		finalNoncer, init = ns.noncers.Load(namespaceId)
		if !init {
			finalNoncer = newNoncer(fallback)
			ns.noncers.Store(namespaceId, finalNoncer)
		}
		ns.mux.Unlock()
	}
	return finalNoncer.(*noncer)
}
