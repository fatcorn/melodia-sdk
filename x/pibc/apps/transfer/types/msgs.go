package types

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// msg types
const (
	TypeMsgTransfer = "transfer"
)

const (
	MaximumReceiverLength = 2048  // maximum length of the receiver address in bytes (value chosen arbitrarily)
	MaximumMemoLength     = 32768 // maximum length of the memo in bytes (value chosen arbitrarily)
)

var (
	_ sdk.Msg = (*MsgTransfer)(nil)
	//_ legacytx.LegacyMsg = (*MsgTransfer)(nil)
)

// NewMsgTransfer creates a new MsgTransfer instance
//
//nolint:interfacer
func NewMsgTransfer(
	srcNamespace, targetNamespace uint64,
	token sdk.Coin, sender, receiver string,
) *MsgTransfer {
	return &MsgTransfer{
		SourceNamespaceId: srcNamespace,
		TargetNamespaceId: targetNamespace,
		Token:             token,
		Sender:            sender,
		Receiver:          receiver,
	}
}

// Type implements legacytx.LegacyMsg
func (MsgTransfer) Type() string {
	return TypeMsgTransfer
}

// Route implements legacytx.LegacyMsg
//func (MsgTransfer) Route() string {
//	return RouterKey
//}

// GetSignBytes implements legacytx.LegacyMsg
//func (msg MsgTransfer) GetSignBytes() []byte {
//	return sdk.MustSortJSON(AminoCdc.MustMarshalJSON(&msg))
//}

// GetSigners implements sdk.Msg
func (msg MsgTransfer) GetSigners() []sdk.AccAddress {
	signer, err := sdk.AccAddressFromBech32(msg.Sender)
	if err != nil {
		panic(err)
	}
	return []sdk.AccAddress{signer}
}
