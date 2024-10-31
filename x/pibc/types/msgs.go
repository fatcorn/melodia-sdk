package types

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
)

var _ sdk.Msg = &MsgRecvPacket{}

// NewMsgRecvPacket constructs new MsgRecvPacket
//
//nolint:interfacer
func NewMsgRecvPacket(
	srcNamespaceId, targetNamespaceId uint64,
	data, sourceTxId []byte,

) *MsgRecvPacket {
	return &MsgRecvPacket{
		Packet: &BasePacket{
			SourceNamespaceId: srcNamespaceId,
			TargetNamespaceId: targetNamespaceId,
			Data:              data,
		},
		SourcePacketTxId: sourceTxId,
	}
}

//// ValidateBasic implements sdk.Msg
//func (msg MsgRecvPacket) ValidateBasic() error {
//	if len(msg.ProofCommitment) == 0 {
//		return sdkerrors.Wrap(commitmenttypes.ErrInvalidProof, "cannot submit an empty proof")
//	}
//	_, err := sdk.AccAddressFromBech32(msg.Signer)
//	if err != nil {
//		return sdkerrors.Wrapf(sdkerrors.ErrInvalidAddress, "string could not be parsed as address: %v", err)
//	}
//	return msg.Packet.ValidateBasic()
//}

//// GetDataSignBytes returns the base64-encoded bytes used for the
//// data field when signing the packet.
//func (msg MsgRecvPacket) GetDataSignBytes() []byte {
//	s := "\"" + base64.StdEncoding.EncodeToString(msg.Packet.Data) + "\""
//	return []byte(s)
//}

// GetSigners implements sdk.Msg
//func (msg MsgRecvPacket) GetSigners() []sdk.AccAddress {
//	signer, err := sdk.AccAddressFromBech32(msg.Signer)
//	if err != nil {
//		panic(err)
//	}
//	return []sdk.AccAddress{signer}
//}

var _ sdk.Msg = &MsgAcknowledgement{}

// NewMsgAcknowledgement constructs a new MsgAcknowledgement
//
//nolint:interfacer
func NewMsgAcknowledgement(
	srcNamespaceId, targetNamespaceId uint64,
	data, soruceTxId []byte,
) *MsgAcknowledgement {
	return &MsgAcknowledgement{
		Packet: &BasePacket{
			SourceNamespaceId: srcNamespaceId,
			TargetNamespaceId: targetNamespaceId,
			Data:              data,
		},
		SourcePacketTxId: soruceTxId,
	}
}
