package mempool

import (
	"container/heap"
	"context"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/huandu/skiplist"
	"math"
	"sort"
	"time"
)

var (
	_ Mempool  = (*PriceNonceMempool[int64])(nil)
	_ Iterator = (*PriceNonceIterator[int64])(nil)
)

// PriceNonceMempool defines a price-nonce mempool.
type (
	PriceNonceMempoolConfig[C comparable] struct {
		// TxPrice defines the transaction priority and comparator.
		TxPrice TxPrice[C]
		// SignerExtractor is an implementation which retrieves signer data from a sdk.MempoolTx
		SignerExtractor SignerExtractionAdapter
	}
	PriceNonceMempool[C comparable] struct {
		cfg PriceNonceMempoolConfig[C]
		txs []sdk.MempoolTx
	}

	TxPrice[C comparable] struct {
		// GetTxPrice returns the price of the transaction. A price must be
		// comparable via Compare.
		GetTxPrice func(tx sdk.MempoolTx) C

		// Compare compares two transaction priorities. The result must be 0 if
		// a == b, -1 if a < b, and +1 if a > b.
		Compare func(a, b C) int

		// MinValue defines the minimum priority value, e.g. MinInt64. This value is
		// used when instantiating a new iterator and comparing weights.
		MinValue C
	}

	Transaction[C comparable] struct {
		// nonce is the sender's sequence number
		Nonce uint64
		// price is the transaction's price
		Price C
		// sender is the transaction's sender
		Sender string
		Tx     sdk.MempoolTx
	}

	PriceNonceIterator[C comparable] struct {
		txs   []*Transaction[C]
		index int
	}
)

func NewDefaultTxPrice() TxPrice[int64] {
	return TxPrice[int64]{
		GetTxPrice: func(tx sdk.MempoolTx) int64 {
			if tx.GasPrice != nil {
				return tx.GasPrice.Int64()
			}
			feeTx, ok := tx.Tx.(sdk.FeeTx)
			if ok {
				fee := feeTx.GetFee()
				gas := feeTx.GetGas()
				var priority int64
				for _, c := range fee {
					p := int64(math.MaxInt64)
					gasPrice := c.Amount.QuoRaw(int64(gas))
					if gasPrice.IsInt64() {
						p = gasPrice.Int64()
					}
					if priority == 0 || p < priority {
						priority = p
					}
				}
				return priority
			}
			return int64(math.MaxInt64)
		},
		Compare: func(a, b int64) int {
			return skiplist.Int64.Compare(a, b)
		},
		MinValue: math.MinInt64,
	}
}
func DefaultPriceNonceMempoolConfig() PriceNonceMempoolConfig[int64] {
	return PriceNonceMempoolConfig[int64]{
		TxPrice:         NewDefaultTxPrice(),
		SignerExtractor: NewDefaultSignerExtractionAdapter(),
	}
}

func NewPriceNonceMempool[C comparable](cfg PriceNonceMempoolConfig[C]) *PriceNonceMempool[C] {
	if cfg.SignerExtractor == nil {
		cfg.SignerExtractor = NewDefaultSignerExtractionAdapter()
	}
	mp := &PriceNonceMempool[C]{
		cfg: cfg,
	}
	return mp
}

func DefaultPriceNonceMempool() *PriceNonceMempool[int64] {
	return NewPriceNonceMempool(DefaultPriceNonceMempoolConfig())
}

func (mp *PriceNonceMempool[C]) Insert(ctx context.Context, tx sdk.MempoolTx) error {
	return nil
}
func (mp *PriceNonceMempool[C]) PrepareSenderTxs() (map[string][]*Transaction[C], []string) {
	t0 := time.Now()
	senderTxs := make(map[string][]*Transaction[C])
	for _, tx := range mp.txs {
		transaction := mp.ToTransaction(tx)
		senderTxs[transaction.Sender] = append(senderTxs[transaction.Sender], &transaction)
	}
	senders := make([]string, 0, len(senderTxs))
	for sender, _ := range senderTxs {
		senders = append(senders, sender)
	}
	sort.Strings(senders)

	println("PrepareSenderTxs use=", time.Since(t0).String(), "size=", len(mp.txs))
	return senderTxs, senders
}
func (mp *PriceNonceMempool[C]) Select(ctx context.Context, txs []sdk.MempoolTx) Iterator {
	mp.txs = txs
	senderTxs, accKeys := mp.PrepareSenderTxs()
	// Separate the transactions by account and sort by nonce
	byNonce := senderTxs
	for _, acc := range accKeys {
		accTxs := byNonce[acc]
		sort.Sort(TxByNonce[C](accTxs))
	}
	// Initialize a price based heap with the head transactions
	byPrice := make(TxByPrice[C], 0, len(byNonce))
	for _, acc := range accKeys {
		accTxs := byNonce[acc]
		byPrice = append(byPrice, accTxs[0])
		byNonce[acc] = accTxs[1:]
	}
	heap.Init(&byPrice)
	// Merge by replacing the best with the next from the same account
	sortedTxs := make([]*Transaction[C], 0)
	for len(byPrice) > 0 {
		// Retrieve the next best transaction by price
		best := heap.Pop(&byPrice).(*Transaction[C])

		// Push in its place the next transaction from the same account
		acc := best.Sender // we only sort valid txs so this cannot fail
		if accTxs, ok := byNonce[acc]; ok && len(accTxs) > 0 {
			heap.Push(&byPrice, accTxs[0])
			byNonce[acc] = accTxs[1:]
		}
		// Accumulate the best priced transaction
		sortedTxs = append(sortedTxs, best)
	}
	iterator := &PriceNonceIterator[C]{
		txs: sortedTxs,
	}
	return iterator
}

func (mp *PriceNonceMempool[C]) CountTx() int {
	return len(mp.txs)
}
func (mp *PriceNonceMempool[C]) Remove(ctx context.Context, tx sdk.MempoolTx) error {
	panic("implement me")
}

func (mp *PriceNonceMempool[C]) ToTransaction(tx sdk.MempoolTx) Transaction[C] {
	price := mp.cfg.TxPrice.GetTxPrice(tx)
	sortTx := Transaction[C]{Nonce: tx.Nonce, Price: price, Sender: tx.From, Tx: tx}
	return sortTx
}
func (i *PriceNonceIterator[C]) Next() Iterator {

	if len(i.txs) == 0 || i.index+1 >= len(i.txs) {
		return nil
	}
	if i.index < len(i.txs) {
		iterator := &PriceNonceIterator[C]{
			txs:   i.txs,
			index: i.index + 1,
		}
		return iterator
	} else {
		return nil
	}
}

func (i *PriceNonceIterator[C]) Tx() sdk.MempoolTx {
	return i.txs[i.index].Tx
}

type Transactions[C comparable] []*Transaction[C]
type TxByNonce[C comparable] Transactions[C]

func (s TxByNonce[C]) Len() int           { return len(s) }
func (s TxByNonce[C]) Less(i, j int) bool { return s[i].Nonce < s[j].Nonce }
func (s TxByNonce[C]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// TxByPrice implements both the sort and the heap interface, making it useful
// for all at once sorting as well as individually adding and removing elements.
type TxByPrice[C comparable] Transactions[C]

func (s TxByPrice[C]) Len() int { return len(s) }
func (s TxByPrice[C]) Less(i, j int) bool {
	cmp := skiplist.Int64.Compare(s[i].Price, s[j].Price)
	if cmp == 0 {
		return s[i].Sender < s[j].Sender
	}
	return cmp > 0
}
func (s TxByPrice[C]) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s *TxByPrice[C]) Push(x interface{}) {
	*s = append(*s, x.(*Transaction[C]))
}

func (s *TxByPrice[C]) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	*s = old[0 : n-1]
	return x
}
