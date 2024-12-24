package selector

import (
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

func BaseFeeSelector(txs []sdk.MempoolTx, nsTable []*cmtproto.NamespaceTableEntries) ([]sdk.MempoolTx, []*cmtproto.NamespaceTableEntries) {
	return txs, nsTable
	//
	//var (
	//	selectedTxs     = make([]sdk.MempoolTx, 0, len(txs))
	//	selectedNsTable = make([]*cmtproto.NamespaceTableEntries, 0)
	//	offset          = int32(0)
	//	iterOffset      = int32(0)
	//)
	//
	//for _, nsEntry := range nsTable {
	//
	//	memTxs := txs[iterOffset:nsEntry.Offset]
	//	namespaceId := nsEntry.Namespace
	//
	//	for _, memTx := range memTxs {
	//		isFeeVerified := memTx.IsBaseFeeVerified
	//		if !isFeeVerified {
	//			continue
	//		}
	//		selectedTxs = append(selectedTxs, memTx)
	//		offset++
	//	}
	//	selectedNsTable = append(selectedNsTable, &cmtproto.NamespaceTableEntries{
	//		Namespace: namespaceId,
	//		Offset:    offset,
	//	})
	//	iterOffset = nsEntry.Offset
	//}
	//
	//return selectedTxs, selectedNsTable
}
