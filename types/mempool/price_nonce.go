package mempool

import (
	"container/heap"
	"context"
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/huandu/skiplist"
	"math"
	"sort"
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
		// SignerExtractor is an implementation which retrieves signer data from a sdk.Tx
		SignerExtractor SignerExtractionAdapter
	}
	PriceNonceMempool[C comparable] struct {
		cfg       PriceNonceMempoolConfig[C]
		senderTxs map[string][]*Transaction[C]
	}

	TxPrice[C comparable] struct {
		// GetTxPrice returns the price of the transaction. A price must be
		// comparable via Compare.
		GetTxPrice func(tx sdk.Tx) C

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
		Tx     sdk.Tx
	}

	PriceNonceIterator[C comparable] struct {
		txs   []*Transaction[C]
		index int
	}
)

func NewDefaultTxPrice() TxPrice[int64] {
	return TxPrice[int64]{
		GetTxPrice: func(tx sdk.Tx) int64 {
			feeTx, ok := tx.(sdk.FeeTx)
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
		cfg:       cfg,
		senderTxs: make(map[string][]*Transaction[C]),
	}
	return mp
}

func DefaultPriceNonceMempool() *PriceNonceMempool[int64] {
	return NewPriceNonceMempool(DefaultPriceNonceMempoolConfig())
}

func (mp *PriceNonceMempool[C]) Insert(_ context.Context, tx sdk.Tx) error {
	transaction, err := mp.ToTransaction(tx)
	if err != nil {
		return err
	}
	mp.senderTxs[transaction.Sender] = append(mp.senderTxs[transaction.Sender], &transaction)
	return nil
}
func (mp *PriceNonceMempool[C]) Select(context.Context, [][]byte) Iterator {
	// Separate the transactions by account and sort by nonce
	byNonce := mp.senderTxs
	for _, accTxs := range byNonce {
		sort.Sort(TxByNonce[C](accTxs))
	}
	// Initialize a price based heap with the head transactions
	byPrice := make(TxByPrice[C], 0, len(byNonce))
	for acc, accTxs := range byNonce {
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
	count := 0
	for _, txs := range mp.senderTxs {
		count += len(txs)
	}
	return count
}
func (mp *PriceNonceMempool[C]) Remove(tx sdk.Tx) error {

	//for _, txs := range mp.txs {
	//	count += len(txs)
	//}
	panic("implement me")
}

func (mp *PriceNonceMempool[C]) ToTransaction(tx sdk.Tx) (Transaction[C], error) {

	sigs, err := mp.cfg.SignerExtractor.GetSigners(tx)
	if err != nil {
		return Transaction[C]{}, err
	}
	if len(sigs) == 0 {
		return Transaction[C]{}, fmt.Errorf("tx must have at least one signer")
	}

	sig := sigs[0]
	sender := sig.Signer.String()
	price := mp.cfg.TxPrice.GetTxPrice(tx)
	nonce := sig.Sequence
	sortTx := Transaction[C]{Nonce: nonce, Price: price, Sender: sender, Tx: tx}
	return sortTx, nil
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

func (i *PriceNonceIterator[C]) Tx() sdk.Tx {
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

func (s TxByPrice[C]) Len() int           { return len(s) }
func (s TxByPrice[C]) Less(i, j int) bool { return skiplist.Int64.Compare(s[i].Price, s[j].Price) > 0 }
func (s TxByPrice[C]) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

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
