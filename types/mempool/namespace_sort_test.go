package mempool_test

import (
	"encoding/json"
	"fmt"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"

	"cosmossdk.io/log"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/mempool"
	simtypes "github.com/cosmos/cosmos-sdk/types/simulation"
)

func (s *MempoolTestSuite) TestNamespaceTxOrder() {
	namespaceIds := make(map[uint64]string)
	namespaceIds[9000] = "gas"
	namespaceIds[9001] = "nonce"

	t := s.T()
	ctx := sdk.NewContext(nil, cmtproto.Header{}, false, log.NewNopLogger())

	newSortBytes, _ := json.Marshal(namespaceIds)
	consensusParams := cmtproto.ConsensusParams{
		Mempool: &cmtproto.MempoolParams{
			Sort: string(newSortBytes),
		},
	}
	ctx = ctx.WithConsensusParams(consensusParams)

	accounts := simtypes.RandomAccounts(rand.New(rand.NewSource(0)), 5)
	sa := accounts[0].Address
	sb := accounts[1].Address

	tests := []struct {
		txs   []txSpec
		order map[uint64][]int
		fail  bool
	}{
		{
			txs: []txSpec{
				{p: 21, n: 4, a: sa},
				{p: 8, n: 3, a: sa},
				{p: 6, n: 2, a: sa},
				{p: 15, n: 1, a: sb},
				{p: 20, n: 1, a: sa},
			},
			order: map[uint64][]int{
				9000: {4, 3, 2, 1, 0},
				9001: {3, 4, 2, 1, 0},
			},
		},
	}
	for i, tt := range tests {
		for namespaceId, _ := range namespaceIds {
			ctx.SetNamespace(namespaceId)
			t.Run(fmt.Sprintf("case %d", i), func(t *testing.T) {
				pool := mempool.NewNamespaceSort()

				// create test txs and insert into mempool
				nsTable := make([]*cmtproto.NamespaceTableEntries, 0, len(tt.txs))
				offset := 0
				txBytes := make([][]byte, 0, len(tt.txs))
				for i, ts := range tt.txs {
					tx := testTx{id: i, priority: int64(ts.p), nonce: uint64(ts.n), address: ts.a}
					c := ctx.WithPriority(tx.priority)
					err := pool.Insert(c, tx)
					require.NoError(t, err)
					txBytes = append(txBytes, nil)
					offset++
				}
				nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
					Namespace: namespaceId,
					Offset:    int32(offset),
				})
				nsIter, _ := pool.Select(ctx, txBytes, nsTable)

				orderedTxs := fetchTxs(nsIter[namespaceId], 100000000)

				var txOrder []int
				for _, tx := range orderedTxs {
					txOrder = append(txOrder, tx.(testTx).id)
				}

				require.Equal(t, tt.order[namespaceId], txOrder)
				require.NoError(t, validateOrder(orderedTxs))

				for _, tx := range orderedTxs {
					require.NoError(t, pool.Remove(tx))
				}

				require.NoError(t, mempool.IsEmpty[int64](pool.GetPool(namespaceId)))
			})

		}

	}

}
