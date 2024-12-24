package selector

import (
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

func BaseFeeSelector(ctx sdk.Context, txs []sdk.MempoolTx, nsTable []*cmtproto.NamespaceTableEntries) ([]sdk.MempoolTx, []*cmtproto.NamespaceTableEntries) {
	return txs, nsTable

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
	//
	//return selectedTxs, selectedNsTable
}
