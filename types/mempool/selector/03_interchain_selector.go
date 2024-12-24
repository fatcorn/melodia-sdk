package selector

import (
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

func InterChainSelector(txs []sdk.MempoolTx, nsTable []*cmtproto.NamespaceTableEntries) ([]sdk.MempoolTx, []*cmtproto.NamespaceTableEntries) {

	var (
		checkInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
		beforeIterOffset       = int32(0)
	)

	for _, nsEntry := range nsTable {
		memTxs := txs[beforeIterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace
		for _, memTx := range memTxs {
			if memTx.IsCross {
				checkInterChainPairTxs[memTx.TxId] = append(checkInterChainPairTxs[memTx.TxId], &cmtproto.NsInterChainTransactionIndex{
					Namespace: namespaceId,
					Index:     int32(memTx.TxIndex),
				})
			}
		}
		beforeIterOffset = nsEntry.Offset
	}
	if len(checkInterChainPairTxs) == 0 {
		return txs, nsTable
	}
	var (
		selectedTxs     = make([]sdk.MempoolTx, 0, len(txs))
		selectedNsTable = make([]*cmtproto.NamespaceTableEntries, 0)

		offset          = int32(0)
		afterIterOffset = int32(0)
	)

	for _, nsEntry := range nsTable {

		memTxs := txs[afterIterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace

		for _, memTx := range memTxs {
			if memTx.IsCross {
				txId := memTx.TxId
				if pairTxs, inPair := checkInterChainPairTxs[txId]; inPair {
					if len(pairTxs) != 2 {
						continue
					}
				}
			}
			selectedTxs = append(selectedTxs, memTx)
			offset++
		}
		selectedNsTable = append(selectedNsTable, &cmtproto.NamespaceTableEntries{
			Namespace: namespaceId,
			Offset:    offset,
		})
		afterIterOffset = nsEntry.Offset
	}
	return selectedTxs, selectedNsTable
}
