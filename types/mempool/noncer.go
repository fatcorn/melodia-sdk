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
		nonces:   multiversion.NewShardedMap(1024),
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
	nonce2, ok := txn.nonces.Get(addr)
	if ok {
		return nonce2.(uint64)
	}
	return 0
}

// set inserts a new virtual nonce into the virtual state database to be returned
// whenever the pool requests it instead of reaching into the real state database.
func (txn *noncer) set(_ sdk.Context, addr string, nonce uint64) {
	txn.nonces.Set(addr, nonce)
}

// setIfLower updates a new virtual nonce into the virtual state database if the
// new one is lower.
func (txn *noncer) setIfLower(ctx sdk.Context, addr string, nonce uint64) {
	if _, ok := txn.nonces.Get(addr); !ok {
		nonce, err := txn.fallback(ctx, addr)
		if err != nil {
			panic(err)
		}
		if nonce != 0 {
			txn.nonces.Set(addr, nonce)
		}
	}
	nonce2, ex := txn.nonces.Get(addr)
	if ex {
		if nonce2.(uint64) <= nonce {
			return
		}
	}
	txn.nonces.Set(addr, nonce)
}

// setAll sets the nonces for all accounts to the given map.
func (txn *noncer) setAll(all map[string]uint64) {
	txn.nonces = multiversion.NewShardedMap(1024)
}
