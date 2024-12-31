package selector

import (
	tmbytes "github.com/cometbft/cometbft/libs/bytes"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

func InterChainSelector(ctx sdk.Context, nsSelectedTxs sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs {

	txs := nsSelectedTxs.Txs
	nsTable := nsSelectedTxs.NsTable

	var (
		checkInterChainPairTxs = make(map[string][]*cmtproto.NsInterChainTransactionIndex)
		beforeIterOffset       = int32(0)
		crossTxsId             = make(map[sdk.Tx]string)
	)

	for _, nsEntry := range nsTable {
		memTxs := txs[beforeIterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace
		for _, memTx := range memTxs {
			txIdBz, isCross, _ := memTx.CrossNamespaceTransactionIdentifierHandler(ctx.WithTxBytes(memTx.TxBytes), memTx.Tx)
			if isCross {
				txId := tmbytes.HexBytes(txIdBz).String()
				crossTxsId[memTx.Tx] = txId
				checkInterChainPairTxs[txId] = append(checkInterChainPairTxs[txId], &cmtproto.NsInterChainTransactionIndex{
					Namespace: namespaceId,
				})
			}
		}
		beforeIterOffset = nsEntry.Offset
	}
	if len(checkInterChainPairTxs) == 0 {
		return nsSelectedTxs
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
			if txId, ok := crossTxsId[memTx.Tx]; ok {
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

	return sdk.NamespaceSelectedTxs{
		Txs:             selectedTxs,
		NsTable:         selectedNsTable,
		ReplacedTxs:     nsSelectedTxs.ReplacedTxs,
		ReplacedNsTable: nsSelectedTxs.ReplacedNsTable,
	}
}
