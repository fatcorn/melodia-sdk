package types

import "github.com/cometbft/cometbft/crypto/tmhash"

func (msgPacket *MsgTransfer) TransactionIdentifier(txPlain []byte) []byte {
	return tmhash.Sum(txPlain)
}
