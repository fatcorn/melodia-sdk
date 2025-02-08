package selector

import (
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

func NonceSelector(ctx sdk.Context, nsSelectedTxs sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs {

	txs := nsSelectedTxs.Txs
	nsTable := nsSelectedTxs.NsTable

	var (
		selectedTxs     = make([]sdk.MempoolTx, 0, len(txs))
		selectedNsTable = make([]*cmtproto.NamespaceTableEntries, 0)
		offset          = int32(0)

		replaceTxs     = make([]sdk.MempoolTx, 0)
		replaceNsTable = make([]*cmtproto.NamespaceTableEntries, 0)
		replaceOffset  = int32(0)

		iterOffset  = int32(0)
		pendingSize = 0
	)

	for _, nsEntry := range nsTable {

		memTxs := txs[iterOffset:nsEntry.Offset]
		namespaceId := nsEntry.Namespace

		var selectedTxsNums int
		selectedTxsSignersSeqs := make(map[string]uint64)
		txsLen := 0

		for _, memTx := range memTxs {
			if memTx.IsNoOp {
				selectedTxs = append(selectedTxs, memTx)
				offset++
				continue
			}
			from, nonce := memTx.From, memTx.Nonce

			txSignersSeqs := make(map[string]uint64)
			seq, ok := selectedTxsSignersSeqs[from]
			if !ok {
				isGapped, isLower := memTx.IsGapped(ctx.WithNamepace(namespaceId), memTx)
				if !isGapped {
					if isLower {
						replaceTxs = append(replaceTxs, memTx)
						replaceOffset++
					} else {
						pendingSize++
					}
					continue
				}
				txSignersSeqs[from] = nonce
			} else {
				// If we have seen this signer before in this block, we must make
				// sure that the current sequence is seq+1; otherwise is invalid
				// and we skip it.
				if seq+1 != nonce {
					if seq+1 > nonce {
						replaceTxs = append(replaceTxs, memTx)
						replaceOffset++
					} else {
						pendingSize++
					}
					continue
				}
				txSignersSeqs[from] = nonce
			}

			selectedTxs = append(selectedTxs, memTx)
			offset++
			txsLen++
			for sender, senderSeq := range txSignersSeqs {
				// If txsLen != selectedTxsNums is true, it means that we've
				// added a new tx to the selected txs, so we need to update
				// the sequence of the sender.
				if txsLen != selectedTxsNums {
					selectedTxsSignersSeqs[sender] = senderSeq
				} else if _, ok := selectedTxsSignersSeqs[sender]; !ok {
					// The transaction hasn't been added but it passed the
					// verification, so we know that the sequence is correct.
					// So we set this sender's sequence to seq-1, in order
					// to avoid unnecessary calls to PrepareProposalVerifyTx.
					selectedTxsSignersSeqs[sender] = senderSeq - 1
				}
			}
			selectedTxsNums = txsLen
		}

		if len(selectedTxs) > 0 {
			selectedNsTable = append(selectedNsTable, &cmtproto.NamespaceTableEntries{
				Namespace: namespaceId,
				Offset:    offset,
			})
		}

		if len(replaceTxs) > 0 {
			replaceNsTable = append(replaceNsTable, &cmtproto.NamespaceTableEntries{
				Namespace: namespaceId,
				Offset:    replaceOffset,
			})
		}

		iterOffset = nsEntry.Offset
	}

	if len(replaceTxs) > 0 {
		ctx.Logger().Info("nonce selector replace txs", "size", len(txs), "nsTable", nsTable, "replaceTxs", len(replaceTxs), "replaceNsTable", replaceNsTable, "height", ctx.BlockHeight())
	}
	if pendingSize > 0 {
		ctx.Logger().Info("nonce selector pending txs", "size", len(txs), "nsTable", nsTable, "pendingSize", pendingSize, "height", ctx.BlockHeight())
	}

	return sdk.NamespaceSelectedTxs{
		Txs:             selectedTxs,
		NsTable:         selectedNsTable,
		ReplacedTxs:     replaceTxs,
		ReplacedNsTable: replaceNsTable,
	}
}
