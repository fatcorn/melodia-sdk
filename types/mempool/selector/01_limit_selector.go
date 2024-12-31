package selector

import (
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"math"
)

func LimitSelector(ctx sdk.Context, maxBytes, maxGas int64, nsSelectedTxs sdk.NamespaceSelectedTxs) sdk.NamespaceSelectedTxs {
	selectedTxs, selectedNsTable := ReapMaxBytesMaxGasNsTable(maxBytes, maxGas, nsSelectedTxs.Txs, nsSelectedTxs.NsTable)
	return sdk.NamespaceSelectedTxs{
		Txs:             selectedTxs,
		NsTable:         selectedNsTable,
		ReplacedTxs:     nsSelectedTxs.ReplacedTxs,
		ReplacedNsTable: nsSelectedTxs.ReplacedNsTable,
	}
}

// ReapMaxBytesMaxGasNsTable safely handles concurrent use by multiple goroutines.
func ReapMaxBytesMaxGasNsTable(maxBytes, maxGas int64, txs []sdk.MempoolTx, nsTable []*cmtproto.NamespaceTableEntries) ([]sdk.MempoolTx, []*cmtproto.NamespaceTableEntries) {
	var (
		overflowRatio float64
		totalBytes    int64
		totalGas      int64
		nsMemTxs      []sdk.NamespaceMempoolTxs
	)

	// List the transactions in namespaces and their total sizes.
	nsMemTxs, totalBytes, totalGas = ListNamespaceMemTxsAndTotalBytes(txs, nsTable)

	// Early exit if no overflow conditions are met.
	if (maxBytes == -1 || totalBytes <= maxBytes) && (maxGas == -1 || totalGas <= maxGas) {
		return txs, nsTable
	}

	// Initialize ratios and compute the overflow ratios only if applicable.
	overflowBytesRatio := 0.0
	overflowGasRatio := 0.0

	if maxBytes > -1 && totalBytes > maxBytes {
		overflowBytesRatio = float64(maxBytes) / float64(totalBytes)
	}

	if maxGas > -1 && totalGas > maxGas {
		overflowGasRatio = float64(maxGas) / float64(totalGas)
	}

	// Choose the larger overflow ratio.
	overflowRatio = math.Max(overflowBytesRatio, overflowGasRatio)

	// Prepare the new list of transactions with the adjusted limits.
	nsTxs := make([]sdk.NamespaceMempoolTxs, 0, len(nsMemTxs))

	for _, nsMemTx := range nsMemTxs {
		memTxs := nsMemTx.Txs
		namespaceId := nsMemTx.NamespaceId

		// Apply the limit based on the overflow ratio.
		limit := len(memTxs)
		if overflowRatio > 0 {
			limit = int(math.Ceil(overflowRatio * float64(len(memTxs))))
		}

		// Limit the number of transactions in the namespace if necessary.
		if limit < len(memTxs) {
			memTxs = memTxs[:limit]
		}

		// Append the adjusted namespace transactions.
		nsTxs = append(nsTxs, sdk.NamespaceMempoolTxs{
			NamespaceId: namespaceId,
			Txs:         memTxs,
		})
	}

	// Reap the transactions and return the result.
	return ReapNsTxs(nsTxs, maxBytes, maxGas)
}

// ReapNsTxs reap namespace txs result
func ReapNsTxs(nsTxs []sdk.NamespaceMempoolTxs, maxBytes, maxGas int64) ([]sdk.MempoolTx, []*cmtproto.NamespaceTableEntries) {
	var (
		totalGas    int64
		runningSize int64
		nsTable     []*cmtproto.NamespaceTableEntries
	)
	// TODO: we will get a performance boost if we have a good estimate of avg
	// size per tx, and set the initial capacity based off of that.
	// txs := make([]types.Tx, 0, cmtmath.MinInt(mem.txs.Len(), max/mem.avgTxSize))
	txs := make([]sdk.MempoolTx, 0)

	offset := 0
	for _, nsTx := range nsTxs {
		memTxs := nsTx.Txs
		for _, memTx := range memTxs {
			dataSize := types.ComputeProtoSizeForTxs([]types.Tx{memTx.TxBytes})
			// Check total size requirement
			if maxBytes > -1 && runningSize+dataSize > maxBytes {
				break
			}
			runningSize += dataSize

			// Check total gas requirement.
			// If maxGas is negative, skip this check.
			// Since newTotalGas < masGas, which
			// must be non-negative, it follows that this won't overflow.
			newTotalGas := totalGas + int64(memTx.TxGas)
			if maxGas > -1 && newTotalGas > maxGas {
				break
			}

			txs = append(txs, memTx)
			totalGas = newTotalGas
			offset++
		}
		nsTable = append(nsTable, &cmtproto.NamespaceTableEntries{
			Namespace: nsTx.NamespaceId,
			Offset:    int32(offset),
		})
	}
	return txs, nsTable
}

func ListNamespaceMemTxsAndTotalBytes(txs []sdk.MempoolTx, nsTable []*cmtproto.NamespaceTableEntries) ([]sdk.NamespaceMempoolTxs, int64, int64) {
	nsTxs := make([]sdk.NamespaceMempoolTxs, 0)
	lastOffset := 0
	totalBytes := int64(0)
	totalGas := int64(0)
	for _, entry := range nsTable {
		namespaceTxs := txs[lastOffset:entry.Offset]
		memTotalBytes, memTotalGas := TotalBytesAndGas(namespaceTxs)
		nsTxs = append(nsTxs, sdk.NamespaceMempoolTxs{
			NamespaceId: entry.Namespace,
			Txs:         namespaceTxs,
		})
		lastOffset = int(entry.Offset)
		totalBytes += memTotalBytes
		totalGas += memTotalGas
	}
	return nsTxs, totalBytes, totalGas
}

func TotalBytesAndGas(txs []sdk.MempoolTx) (int64, int64) {
	totalBytes := int64(0)
	totalGas := int64(0)

	for _, tx := range txs {
		if tx.Tx != nil {
			totalBytes += int64(len(tx.TxBytes))
			totalGas += int64(tx.TxGas)
		}
	}
	return totalBytes, totalGas
}
