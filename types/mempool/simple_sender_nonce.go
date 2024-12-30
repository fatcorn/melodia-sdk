package mempool

import (
	"context"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"sort"
	"time"
)

var (
	_ Mempool  = (*SimpleSenderNonceMempool[int64])(nil)
	_ Iterator = (*SimpleSenderNonceIterator[int64])(nil)
)

// SimpleSenderNonceMempool defines a price-nonce mempool.
type (
	SimpleSenderNonceMempoolConfig[C comparable] struct {
		// SignerExtractor is an implementation which retrieves signer data from a sdk.MempoolTx
		SignerExtractor SignerExtractionAdapter
	}
	SimpleSenderNonceMempool[C comparable] struct {
		cfg SimpleSenderNonceMempoolConfig[C]
		txs []sdk.MempoolTx
	}
	SimpleSenderNonceIterator[C comparable] struct {
		txs   []*Transaction[C]
		index int
	}
)

func DefaultSimpleSenderNonceMempoolConfig() SimpleSenderNonceMempoolConfig[int64] {
	return SimpleSenderNonceMempoolConfig[int64]{
		SignerExtractor: NewDefaultSignerExtractionAdapter(),
	}
}

func NewSimpleSenderNonceMempool[C comparable](cfg SimpleSenderNonceMempoolConfig[C]) *SimpleSenderNonceMempool[C] {
	if cfg.SignerExtractor == nil {
		cfg.SignerExtractor = NewDefaultSignerExtractionAdapter()
	}
	mp := &SimpleSenderNonceMempool[C]{
		cfg: cfg,
	}
	return mp
}

func DefaultSimpleSenderNonceMempool() *SimpleSenderNonceMempool[int64] {
	return NewSimpleSenderNonceMempool(DefaultSimpleSenderNonceMempoolConfig())
}

func (mp *SimpleSenderNonceMempool[C]) Insert(ctx context.Context, tx sdk.MempoolTx) error {
	return nil
}
func (mp *SimpleSenderNonceMempool[C]) PrepareSenderTxs() (map[string][]*Transaction[C], []string) {
	t0 := time.Now()
	senderTxs := make(map[string][]*Transaction[C])
	for _, tx := range mp.txs {
		transaction := Transaction[C]{Nonce: tx.Nonce, Sender: tx.From, Tx: tx}
		senderTxs[transaction.Sender] = append(senderTxs[transaction.Sender], &transaction)
	}
	senders := make([]string, 0, len(senderTxs))
	for sender, _ := range senderTxs {
		senders = append(senders, sender)
	}
	sort.Strings(senders)

	println("SimpleSenderNonceMempool->PrepareSenderTxs use=", time.Since(t0).String(), "size=", len(mp.txs))
	return senderTxs, senders
}
func (mp *SimpleSenderNonceMempool[C]) Select(ctx context.Context, txs []sdk.MempoolTx) Iterator {
	mp.txs = txs
	senderTxs, accKeys := mp.PrepareSenderTxs()
	// Separate the transactions by account and sort by nonce
	byNonce := senderTxs
	for _, acc := range accKeys {
		accTxs := byNonce[acc]
		sort.Sort(TxByNonce[C](accTxs))
	}
	sortedTxs := make([]*Transaction[C], 0)
	for _, acc := range accKeys {
		accTxs := byNonce[acc]
		sortedTxs = append(sortedTxs, accTxs...)
	}
	iterator := &SimpleSenderNonceIterator[C]{
		txs: sortedTxs,
	}
	return iterator
}

func (mp *SimpleSenderNonceMempool[C]) CountTx() int {
	return len(mp.txs)
}
func (mp *SimpleSenderNonceMempool[C]) Remove(ctx context.Context, tx sdk.MempoolTx) error {
	panic("implement me")
}

func (i *SimpleSenderNonceIterator[C]) Next() Iterator {

	if len(i.txs) == 0 || i.index+1 >= len(i.txs) {
		return nil
	}
	if i.index < len(i.txs) {
		iterator := &SimpleSenderNonceIterator[C]{
			txs:   i.txs,
			index: i.index + 1,
		}
		return iterator
	} else {
		return nil
	}
}

func (i *SimpleSenderNonceIterator[C]) Tx() sdk.MempoolTx {
	return i.txs[i.index].Tx
}
