package selector

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
)

func BaseFeeSelector(ctx sdk.Context, nsSelectTxs sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs {
	return nsSelectTxs
	//
	//txs := nsSelectTxs.Txs
	//nsTable := nsSelectTxs.NsTable
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
	//	var baseFee *big.Int
	//	for _, memTx := range memTxs {
	//		if baseFee == nil {
	//			baseFee = memTx.BaseFeeHandler(ctx.WithNamepace(namespaceId))
	//		}
	//		isFeeVerified := memTx.CheckTxFeeHandler(ctx, baseFee, memTx.Tx)
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
	//return sdk.NamespaceSelectedTxs{
	//	Txs:            selectedTxs,
	//	NsTable:        selectedNsTable,
	//	ReplaceTxs:     nsSelectTxs.ReplaceTxs,
	//	ReplaceNsTable: nsSelectTxs.ReplaceNsTable,
	//}
}
