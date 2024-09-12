package baseapp

import (
	"bytes"
	basev1beta1 "cosmossdk.io/api/cosmos/base/v1beta1"
	multisigv1beta1 "cosmossdk.io/api/cosmos/crypto/multisig/v1beta1"
	signingv1beta1 "cosmossdk.io/api/cosmos/tx/signing/v1beta1"
	txv1beta1 "cosmossdk.io/api/cosmos/tx/v1beta1"
	txsigning "cosmossdk.io/x/tx/signing"
	"cosmossdk.io/x/tx/signing/aminojson"
	"cosmossdk.io/x/tx/signing/direct"
	"cosmossdk.io/x/tx/signing/directaux"
	"cosmossdk.io/x/tx/signing/textual"
	"fmt"
	"github.com/cosmos/cosmos-sdk/codec/unknownproto"
	authcodec "github.com/cosmos/cosmos-sdk/x/auth/codec"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/cosmos/gogoproto/proto"
	protov2 "google.golang.org/protobuf/proto"

	errorsmod "cosmossdk.io/errors"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptotypes "github.com/cosmos/cosmos-sdk/crypto/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/cosmos/cosmos-sdk/types/tx"
	txtypes "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/types/tx/signing"
	authsigning "github.com/cosmos/cosmos-sdk/x/auth/signing"
)

// wrapper is a wrapper around the tx.Tx proto.Message which retain the raw
// body and auth_info bytes.
type wrapper struct {
	cdc codec.Codec

	tx *tx.Tx

	// bodyBz represents the protobuf encoding of TxBody. This should be encoding
	// from the client using TxRaw if the tx was decoded from the wire
	bodyBz []byte

	// authInfoBz represents the protobuf encoding of TxBody. This should be encoding
	// from the client using TxRaw if the tx was decoded from the wire
	authInfoBz []byte

	txBodyHasUnknownNonCriticals bool

	signers [][]byte
	msgsV2  []protov2.Message
}

var (
	_ authsigning.Tx   = &wrapper{}
	_ client.TxBuilder = &wrapper{}
	//_ ante.HasExtensionOptionsTx = &wrapper{}
	_ ExtensionOptionsTxBuilder = &wrapper{}
)

// ExtensionOptionsTxBuilder defines a TxBuilder that can also set extensions.
type ExtensionOptionsTxBuilder interface {
	client.TxBuilder

	SetExtensionOptions(...*codectypes.Any)
	SetNonCriticalExtensionOptions(...*codectypes.Any)
}

func newBuilder(cdc codec.Codec) *wrapper {
	w := &wrapper{
		cdc: cdc,
		tx: &tx.Tx{
			Body: &tx.TxBody{},
			AuthInfo: &tx.AuthInfo{
				Fee: &tx.Fee{},
			},
		},
	}
	return w
}

func (w *wrapper) GetMsgs() []sdk.Msg {
	return w.tx.GetMsgs()
}

func (w *wrapper) GetMsgsV2() ([]protov2.Message, error) {
	if w.msgsV2 == nil {
		err := w.initSignersAndMsgsV2()
		if err != nil {
			return nil, err
		}
	}

	return w.msgsV2, nil
}

func (w *wrapper) ValidateBasic() error {
	if w.tx == nil {
		return fmt.Errorf("bad Tx")
	}

	if err := w.tx.ValidateBasic(); err != nil {
		return err
	}

	sigs := w.tx.Signatures
	signers, err := w.GetSigners()
	if err != nil {
		return err
	}

	if len(sigs) != len(signers) {
		return errorsmod.Wrapf(
			sdkerrors.ErrUnauthorized,
			"wrong number of signers; expected %d, got %d", len(signers), len(sigs),
		)
	}

	return nil
}

func (w *wrapper) getBodyBytes() []byte {
	if len(w.bodyBz) == 0 {
		// if bodyBz is empty, then marshal the body. bodyBz will generally
		// be set to nil whenever SetBody is called so the result of calling
		// this method should always return the correct bytes. Note that after
		// decoding bodyBz is derived from TxRaw so that it matches what was
		// transmitted over the wire
		var err error
		w.bodyBz, err = proto.Marshal(w.tx.Body)
		if err != nil {
			panic(err)
		}
	}
	return w.bodyBz
}

func (w *wrapper) getAuthInfoBytes() []byte {
	if len(w.authInfoBz) == 0 {
		// if authInfoBz is empty, then marshal the body. authInfoBz will generally
		// be set to nil whenever SetAuthInfo is called so the result of calling
		// this method should always return the correct bytes. Note that after
		// decoding authInfoBz is derived from TxRaw so that it matches what was
		// transmitted over the wire
		var err error
		w.authInfoBz, err = proto.Marshal(w.tx.AuthInfo)
		if err != nil {
			panic(err)
		}
	}
	return w.authInfoBz
}

func (w *wrapper) initSignersAndMsgsV2() error {
	var err error
	w.signers, w.msgsV2, err = w.tx.GetSigners(w.cdc)
	return err
}

func (w *wrapper) GetSigners() ([][]byte, error) {
	if w.signers == nil {
		err := w.initSignersAndMsgsV2()
		if err != nil {
			return nil, err
		}
	}
	return w.signers, nil
}

func (w *wrapper) GetPubKeys() ([]cryptotypes.PubKey, error) {
	signerInfos := w.tx.AuthInfo.SignerInfos
	pks := make([]cryptotypes.PubKey, len(signerInfos))

	for i, si := range signerInfos {
		// NOTE: it is okay to leave this nil if there is no PubKey in the SignerInfo.
		// PubKey's can be left unset in SignerInfo.
		if si.PublicKey == nil {
			continue
		}

		pkAny := si.PublicKey.GetCachedValue()
		pk, ok := pkAny.(cryptotypes.PubKey)
		if ok {
			pks[i] = pk
		} else {
			return nil, errorsmod.Wrapf(sdkerrors.ErrLogic, "Expecting PubKey, got: %T", pkAny)
		}
	}

	return pks, nil
}

func (w *wrapper) GetGas() uint64 {
	return w.tx.AuthInfo.Fee.GasLimit
}

func (w *wrapper) GetFee() sdk.Coins {
	return w.tx.AuthInfo.Fee.Amount
}

func (w *wrapper) FeePayer() []byte {
	feePayer := w.tx.AuthInfo.Fee.Payer
	if feePayer != "" {
		feePayerAddr, err := w.cdc.InterfaceRegistry().SigningContext().AddressCodec().StringToBytes(feePayer)
		if err != nil {
			panic(err)
		}
		return feePayerAddr
	}

	// use first signer as default if no payer specified
	signers, err := w.GetSigners()
	if err != nil {
		return nil
	}

	return signers[0]
}

func (w *wrapper) FeeGranter() []byte {
	return w.tx.FeeGranter(w.cdc)
}

func (w *wrapper) GetMemo() string {
	return w.tx.Body.Memo
}

// GetTimeoutHeight returns the transaction's timeout height (if set).
func (w *wrapper) GetTimeoutHeight() uint64 {
	return w.tx.Body.TimeoutHeight
}

func (w *wrapper) GetSignaturesV2() ([]signing.SignatureV2, error) {
	signerInfos := w.tx.AuthInfo.SignerInfos
	sigs := w.tx.Signatures
	pubKeys, err := w.GetPubKeys()
	if err != nil {
		return nil, err
	}
	n := len(signerInfos)
	res := make([]signing.SignatureV2, n)

	for i, si := range signerInfos {
		// handle nil signatures (in case of simulation)
		if si.ModeInfo == nil {
			res[i] = signing.SignatureV2{
				PubKey: pubKeys[i],
			}
		} else {
			var err error
			sigData, err := ModeInfoAndSigToSignatureData(si.ModeInfo, sigs[i])
			if err != nil {
				return nil, err
			}
			// sequence number is functionally a transaction nonce and referred to as such in the SDK
			nonce := si.GetSequence()
			res[i] = signing.SignatureV2{
				PubKey:   pubKeys[i],
				Data:     sigData,
				Sequence: nonce,
			}

		}
	}

	return res, nil
}

func (w *wrapper) SetMsgs(msgs ...sdk.Msg) error {
	anys, err := tx.SetMsgs(msgs)
	if err != nil {
		return err
	}

	w.tx.Body.Messages = anys

	// set bodyBz to nil because the cached bodyBz no longer matches tx.Body
	w.bodyBz = nil

	// reset signers and msgsV2
	w.signers = nil
	w.msgsV2 = nil

	return nil
}

// SetTimeoutHeight sets the transaction's height timeout.
func (w *wrapper) SetTimeoutHeight(height uint64) {
	w.tx.Body.TimeoutHeight = height

	// set bodyBz to nil because the cached bodyBz no longer matches tx.Body
	w.bodyBz = nil
}

func (w *wrapper) SetMemo(memo string) {
	w.tx.Body.Memo = memo

	// set bodyBz to nil because the cached bodyBz no longer matches tx.Body
	w.bodyBz = nil
}

func (w *wrapper) SetGasLimit(limit uint64) {
	if w.tx.AuthInfo.Fee == nil {
		w.tx.AuthInfo.Fee = &tx.Fee{}
	}

	w.tx.AuthInfo.Fee.GasLimit = limit

	// set authInfoBz to nil because the cached authInfoBz no longer matches tx.AuthInfo
	w.authInfoBz = nil
}

func (w *wrapper) SetFeeAmount(coins sdk.Coins) {
	if w.tx.AuthInfo.Fee == nil {
		w.tx.AuthInfo.Fee = &tx.Fee{}
	}

	w.tx.AuthInfo.Fee.Amount = coins

	// set authInfoBz to nil because the cached authInfoBz no longer matches tx.AuthInfo
	w.authInfoBz = nil
}

func (w *wrapper) SetFeePayer(feePayer sdk.AccAddress) {
	if w.tx.AuthInfo.Fee == nil {
		w.tx.AuthInfo.Fee = &tx.Fee{}
	}

	w.tx.AuthInfo.Fee.Payer = feePayer.String()

	// set authInfoBz to nil because the cached authInfoBz no longer matches tx.AuthInfo
	w.authInfoBz = nil
}

func (w *wrapper) SetFeeGranter(feeGranter sdk.AccAddress) {
	if w.tx.AuthInfo.Fee == nil {
		w.tx.AuthInfo.Fee = &tx.Fee{}
	}

	w.tx.AuthInfo.Fee.Granter = feeGranter.String()

	// set authInfoBz to nil because the cached authInfoBz no longer matches tx.AuthInfo
	w.authInfoBz = nil
}

func (w *wrapper) SetSignatures(signatures ...signing.SignatureV2) error {
	n := len(signatures)
	signerInfos := make([]*tx.SignerInfo, n)
	rawSigs := make([][]byte, n)

	for i, sig := range signatures {
		var (
			modeInfo *tx.ModeInfo
			pubKey   *codectypes.Any
			err      error
		)
		modeInfo, rawSigs[i] = SignatureDataToModeInfoAndSig(sig.Data)
		if sig.PubKey != nil {
			pubKey, err = codectypes.NewAnyWithValue(sig.PubKey)
			if err != nil {
				return err
			}
		}
		signerInfos[i] = &tx.SignerInfo{
			PublicKey: pubKey,
			ModeInfo:  modeInfo,
			Sequence:  sig.Sequence,
		}
	}

	w.setSignerInfos(signerInfos)
	w.setSignatures(rawSigs)

	return nil
}

func (w *wrapper) setSignerInfos(infos []*tx.SignerInfo) {
	w.tx.AuthInfo.SignerInfos = infos
	// set authInfoBz to nil because the cached authInfoBz no longer matches tx.AuthInfo
	w.authInfoBz = nil
}

func (w *wrapper) setSignerInfoAtIndex(index int, info *tx.SignerInfo) {
	signers, err := w.GetSigners()
	if err != nil {
		panic(err)
	}

	if w.tx.AuthInfo.SignerInfos == nil {
		w.tx.AuthInfo.SignerInfos = make([]*tx.SignerInfo, len(signers))
	}

	w.tx.AuthInfo.SignerInfos[index] = info
	// set authInfoBz to nil because the cached authInfoBz no longer matches tx.AuthInfo
	w.authInfoBz = nil
}

func (w *wrapper) setSignatures(sigs [][]byte) {
	w.tx.Signatures = sigs
}

func (w *wrapper) setSignatureAtIndex(index int, sig []byte) {
	signers, err := w.GetSigners()
	if err != nil {
		panic(err)
	}

	if w.tx.Signatures == nil {
		w.tx.Signatures = make([][]byte, len(signers))
	}

	w.tx.Signatures[index] = sig
}

func (w *wrapper) GetTx() authsigning.Tx {
	return w
}

func (w *wrapper) GetProtoTx() *tx.Tx {
	return w.tx
}

// Deprecated: AsAny extracts proto Tx and wraps it into Any.
// NOTE: You should probably use `GetProtoTx` if you want to serialize the transaction.
func (w *wrapper) AsAny() *codectypes.Any {
	return codectypes.UnsafePackAny(w.tx)
}

// WrapTx creates a TxBuilder wrapper around a tx.Tx proto message.
func WrapTx(protoTx *tx.Tx) client.TxBuilder {
	return &wrapper{
		tx: protoTx,
	}
}

func (w *wrapper) GetExtensionOptions() []*codectypes.Any {
	return w.tx.Body.ExtensionOptions
}

func (w *wrapper) GetNonCriticalExtensionOptions() []*codectypes.Any {
	return w.tx.Body.NonCriticalExtensionOptions
}

func (w *wrapper) SetExtensionOptions(extOpts ...*codectypes.Any) {
	w.tx.Body.ExtensionOptions = extOpts
	w.bodyBz = nil
}

func (w *wrapper) SetNonCriticalExtensionOptions(extOpts ...*codectypes.Any) {
	w.tx.Body.NonCriticalExtensionOptions = extOpts
	w.bodyBz = nil
}

func (w *wrapper) AddAuxSignerData(data tx.AuxSignerData) error {
	err := data.ValidateBasic()
	if err != nil {
		return err
	}

	w.bodyBz = data.SignDoc.BodyBytes

	var body tx.TxBody
	err = w.cdc.Unmarshal(w.bodyBz, &body)
	if err != nil {
		return err
	}

	if w.tx.Body.Memo != "" && w.tx.Body.Memo != body.Memo {
		return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has memo %s, got %s in AuxSignerData", w.tx.Body.Memo, body.Memo)
	}
	if w.tx.Body.TimeoutHeight != 0 && w.tx.Body.TimeoutHeight != body.TimeoutHeight {
		return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has timeout height %d, got %d in AuxSignerData", w.tx.Body.TimeoutHeight, body.TimeoutHeight)
	}
	if len(w.tx.Body.ExtensionOptions) != 0 {
		if len(w.tx.Body.ExtensionOptions) != len(body.ExtensionOptions) {
			return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has %d extension options, got %d in AuxSignerData", len(w.tx.Body.ExtensionOptions), len(body.ExtensionOptions))
		}
		for i, o := range w.tx.Body.ExtensionOptions {
			if !o.Equal(body.ExtensionOptions[i]) {
				return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has extension option %+v at index %d, got %+v in AuxSignerData", o, i, body.ExtensionOptions[i])
			}
		}
	}
	if len(w.tx.Body.NonCriticalExtensionOptions) != 0 {
		if len(w.tx.Body.NonCriticalExtensionOptions) != len(body.NonCriticalExtensionOptions) {
			return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has %d non-critical extension options, got %d in AuxSignerData", len(w.tx.Body.NonCriticalExtensionOptions), len(body.NonCriticalExtensionOptions))
		}
		for i, o := range w.tx.Body.NonCriticalExtensionOptions {
			if !o.Equal(body.NonCriticalExtensionOptions[i]) {
				return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has non-critical extension option %+v at index %d, got %+v in AuxSignerData", o, i, body.NonCriticalExtensionOptions[i])
			}
		}
	}
	if len(w.tx.Body.Messages) != 0 {
		if len(w.tx.Body.Messages) != len(body.Messages) {
			return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has %d Msgs, got %d in AuxSignerData", len(w.tx.Body.Messages), len(body.Messages))
		}
		for i, o := range w.tx.Body.Messages {
			if !o.Equal(body.Messages[i]) {
				return sdkerrors.ErrInvalidRequest.Wrapf("TxBuilder has Msg %+v at index %d, got %+v in AuxSignerData", o, i, body.Messages[i])
			}
		}
	}

	w.SetMemo(body.Memo)
	w.SetTimeoutHeight(body.TimeoutHeight)
	w.SetExtensionOptions(body.ExtensionOptions...)
	w.SetNonCriticalExtensionOptions(body.NonCriticalExtensionOptions...)
	msgs := make([]sdk.Msg, len(body.Messages))
	for i, msgAny := range body.Messages {
		msgs[i] = msgAny.GetCachedValue().(sdk.Msg)
	}
	err = w.SetMsgs(msgs...)
	if err != nil {
		return err
	}

	// Get the aux signer's index in GetSigners.
	signerIndex := -1
	signers, err := w.GetSigners()
	if err != nil {
		return err
	}

	for i, signer := range signers {
		addrBz, err := w.cdc.InterfaceRegistry().SigningContext().AddressCodec().StringToBytes(data.Address)
		if err != nil {
			return err
		}
		if bytes.Equal(signer, addrBz) {
			signerIndex = i
		}
	}
	if signerIndex < 0 {
		return sdkerrors.ErrLogic.Wrapf("address %s is not a signer", data.Address)
	}

	w.setSignerInfoAtIndex(signerIndex, &tx.SignerInfo{
		PublicKey: data.SignDoc.PublicKey,
		ModeInfo:  &tx.ModeInfo{Sum: &tx.ModeInfo_Single_{Single: &tx.ModeInfo_Single{Mode: data.Mode}}},
		Sequence:  data.SignDoc.Sequence,
	})
	w.setSignatureAtIndex(signerIndex, data.Sig)

	return nil
}

// GetSigningTxData returns an x/tx/signing.TxData representation of a transaction for use in the signing
// API defined in x/tx.  The reason for all of this conversion is that x/tx depends on the protoreflect API
// defined in google.golang.org/protobuf while x/auth/tx depends on the legacy proto API defined in
// github.com/gogo/protobuf and the downstream SDK fork of that library, github.com/cosmos/gogoproto.
// Therefore we need to convert between the two APIs.
func (w *wrapper) GetSigningTxData() txsigning.TxData {
	body := w.tx.Body
	authInfo := w.tx.AuthInfo

	msgs := make([]*anypb.Any, len(body.Messages))
	for i, msg := range body.Messages {
		msgs[i] = &anypb.Any{
			TypeUrl: msg.TypeUrl,
			Value:   msg.Value,
		}
	}

	extOptions := make([]*anypb.Any, len(body.ExtensionOptions))
	for i, extOption := range body.ExtensionOptions {
		extOptions[i] = &anypb.Any{
			TypeUrl: extOption.TypeUrl,
			Value:   extOption.Value,
		}
	}

	nonCriticalExtOptions := make([]*anypb.Any, len(body.NonCriticalExtensionOptions))
	for i, extOption := range body.NonCriticalExtensionOptions {
		nonCriticalExtOptions[i] = &anypb.Any{
			TypeUrl: extOption.TypeUrl,
			Value:   extOption.Value,
		}
	}

	feeCoins := authInfo.Fee.Amount
	feeAmount := make([]*basev1beta1.Coin, len(feeCoins))
	for i, coin := range feeCoins {
		feeAmount[i] = &basev1beta1.Coin{
			Denom:  coin.Denom,
			Amount: coin.Amount.String(),
		}
	}

	txSignerInfos := make([]*txv1beta1.SignerInfo, len(authInfo.SignerInfos))
	for i, signerInfo := range authInfo.SignerInfos {
		modeInfo := &txv1beta1.ModeInfo{}
		adaptModeInfo(signerInfo.ModeInfo, modeInfo)
		txSignerInfo := &txv1beta1.SignerInfo{
			PublicKey: &anypb.Any{
				TypeUrl: signerInfo.PublicKey.TypeUrl,
				Value:   signerInfo.PublicKey.Value,
			},
			Sequence: signerInfo.Sequence,
			ModeInfo: modeInfo,
		}
		txSignerInfos[i] = txSignerInfo
	}

	txAuthInfo := &txv1beta1.AuthInfo{
		SignerInfos: txSignerInfos,
		Fee: &txv1beta1.Fee{
			Amount:   feeAmount,
			GasLimit: authInfo.Fee.GasLimit,
			Payer:    authInfo.Fee.Payer,
			Granter:  authInfo.Fee.Granter,
		},
	}

	txBody := &txv1beta1.TxBody{
		Messages:                    msgs,
		Memo:                        body.Memo,
		TimeoutHeight:               body.TimeoutHeight,
		ExtensionOptions:            extOptions,
		NonCriticalExtensionOptions: nonCriticalExtOptions,
	}
	txData := txsigning.TxData{
		AuthInfo:      txAuthInfo,
		AuthInfoBytes: w.getAuthInfoBytes(),
		Body:          txBody,
		BodyBytes:     w.getBodyBytes(),
	}
	return txData
}

func adaptModeInfo(legacy *tx.ModeInfo, res *txv1beta1.ModeInfo) {
	// handle nil modeInfo. this is permissible through the code path:
	// https://github.com/cosmos/cosmos-sdk/blob/4a6a1e3cb8de459891cb0495052589673d14ef51/x/auth/tx/builder.go#L295
	// -> https://github.com/cosmos/cosmos-sdk/blob/b7841e3a76a38d069c1b9cb3d48368f7a67e9c26/x/auth/tx/sigs.go#L15-L17
	// when signature.Data is nil.
	if legacy == nil {
		return
	}

	switch mi := legacy.Sum.(type) {
	case *tx.ModeInfo_Single_:
		res.Sum = &txv1beta1.ModeInfo_Single_{
			Single: &txv1beta1.ModeInfo_Single{
				Mode: signingv1beta1.SignMode(legacy.GetSingle().Mode),
			},
		}
	case *tx.ModeInfo_Multi_:
		multiModeInfos := legacy.GetMulti().ModeInfos
		modeInfos := make([]*txv1beta1.ModeInfo, len(multiModeInfos))
		for _, modeInfo := range multiModeInfos {
			adaptModeInfo(modeInfo, &txv1beta1.ModeInfo{})
		}
		res.Sum = &txv1beta1.ModeInfo_Multi_{
			Multi: &txv1beta1.ModeInfo_Multi{
				Bitarray: &multisigv1beta1.CompactBitArray{
					Elems:           mi.Multi.Bitarray.Elems,
					ExtraBitsStored: mi.Multi.Bitarray.ExtraBitsStored,
				},
				ModeInfos: modeInfos,
			},
		}
	}
}

// SignatureDataToModeInfoAndSig converts a SignatureData to a ModeInfo and raw bytes signature
func SignatureDataToModeInfoAndSig(data signing.SignatureData) (*tx.ModeInfo, []byte) {
	if data == nil {
		return nil, nil
	}

	switch data := data.(type) {
	case *signing.SingleSignatureData:
		return &tx.ModeInfo{
			Sum: &tx.ModeInfo_Single_{
				Single: &tx.ModeInfo_Single{Mode: data.SignMode},
			},
		}, data.Signature
	case *signing.MultiSignatureData:
		n := len(data.Signatures)
		modeInfos := make([]*tx.ModeInfo, n)
		sigs := make([][]byte, n)

		for i, d := range data.Signatures {
			modeInfos[i], sigs[i] = SignatureDataToModeInfoAndSig(d)
		}

		multisig := cryptotypes.MultiSignature{
			Signatures: sigs,
		}
		sig, err := multisig.Marshal()
		if err != nil {
			panic(err)
		}

		return &tx.ModeInfo{
			Sum: &tx.ModeInfo_Multi_{
				Multi: &tx.ModeInfo_Multi{
					Bitarray:  data.BitArray,
					ModeInfos: modeInfos,
				},
			},
		}, sig
	default:
		panic(fmt.Sprintf("unexpected signature data type %T", data))
	}
}

// ModeInfoAndSigToSignatureData converts a ModeInfo and raw bytes signature to a SignatureData or returns
// an error
func ModeInfoAndSigToSignatureData(modeInfo *tx.ModeInfo, sig []byte) (signing.SignatureData, error) {
	switch modeInfo := modeInfo.Sum.(type) {
	case *tx.ModeInfo_Single_:
		return &signing.SingleSignatureData{
			SignMode:  modeInfo.Single.Mode,
			Signature: sig,
		}, nil

	case *tx.ModeInfo_Multi_:
		multi := modeInfo.Multi

		sigs, err := decodeMultisignatures(sig)
		if err != nil {
			return nil, err
		}

		sigv2s := make([]signing.SignatureData, len(sigs))
		for i, mi := range multi.ModeInfos {
			sigv2s[i], err = ModeInfoAndSigToSignatureData(mi, sigs[i])
			if err != nil {
				return nil, err
			}
		}

		return &signing.MultiSignatureData{
			BitArray:   multi.Bitarray,
			Signatures: sigv2s,
		}, nil

	default:
		panic(fmt.Errorf("unexpected ModeInfo data type %T", modeInfo))
	}
}

// decodeMultisignatures safely decodes the raw bytes as a MultiSignature protobuf message
func decodeMultisignatures(bz []byte) ([][]byte, error) {
	multisig := cryptotypes.MultiSignature{}
	err := multisig.Unmarshal(bz)
	if err != nil {
		return nil, err
	}
	// NOTE: it is import to reject multi-signatures that contain unrecognized fields because this is an exploitable
	// malleability in the protobuf message. Basically an attacker could bloat a MultiSignature message with unknown
	// fields, thus bloating the transaction and causing it to fail.
	if len(multisig.XXX_unrecognized) > 0 {
		return nil, fmt.Errorf("rejecting unrecognized fields found in MultiSignature")
	}
	return multisig.Signatures, nil
}

func (g config) MarshalSignatureJSON(sigs []signing.SignatureV2) ([]byte, error) {
	descs := make([]*signing.SignatureDescriptor, len(sigs))

	for i, sig := range sigs {
		descData := signing.SignatureDataToProto(sig.Data)
		any, err := codectypes.NewAnyWithValue(sig.PubKey)
		if err != nil {
			return nil, err
		}

		descs[i] = &signing.SignatureDescriptor{
			PublicKey: any,
			Data:      descData,
			Sequence:  sig.Sequence,
		}
	}

	toJSON := &signing.SignatureDescriptors{Signatures: descs}

	return codec.ProtoMarshalJSON(toJSON, nil)
}

func (g config) UnmarshalSignatureJSON(bz []byte) ([]signing.SignatureV2, error) {
	var sigDescs signing.SignatureDescriptors
	err := g.protoCodec.UnmarshalJSON(bz, &sigDescs)
	if err != nil {
		return nil, err
	}

	sigs := make([]signing.SignatureV2, len(sigDescs.Signatures))
	for i, desc := range sigDescs.Signatures {
		pubKey, _ := desc.PublicKey.GetCachedValue().(cryptotypes.PubKey)

		data := signing.SignatureDataFromProto(desc.Data)

		sigs[i] = signing.SignatureV2{
			PubKey:   pubKey,
			Data:     data,
			Sequence: desc.Sequence,
		}
	}

	return sigs, nil
}

type config struct {
	handler        *txsigning.HandlerMap
	decoder        sdk.TxDecoder
	encoder        sdk.TxEncoder
	jsonDecoder    sdk.TxDecoder
	jsonEncoder    sdk.TxEncoder
	protoCodec     codec.Codec
	signingContext *txsigning.Context
}

func (g config) NewTxBuilder() client.TxBuilder {
	return newBuilder(g.protoCodec)
}

// WrapTxBuilder returns a builder from provided transaction
func (g config) WrapTxBuilder(newTx sdk.Tx) (client.TxBuilder, error) {
	newBuilder, ok := newTx.(*wrapper)
	if !ok {
		return nil, fmt.Errorf("expected %T, got %T", &wrapper{}, newTx)
	}

	return newBuilder, nil
}

func (g config) SignModeHandler() *txsigning.HandlerMap {
	return g.handler
}

func (g config) TxEncoder() sdk.TxEncoder {
	return g.encoder
}

func (g config) TxDecoder() sdk.TxDecoder {
	return g.decoder
}

func (g config) TxJSONEncoder() sdk.TxEncoder {
	return g.jsonEncoder
}

func (g config) TxJSONDecoder() sdk.TxDecoder {
	return g.jsonDecoder
}

func (g config) SigningContext() *txsigning.Context {
	return g.signingContext
}

func NewTxConfig(protoCodec codec.Codec, enabledSignModes []signing.SignMode,
	customSignModes ...txsigning.SignModeHandler,
) client.TxConfig {
	txConfig, err := NewTxConfigWithOptions(protoCodec, ConfigOptions{
		EnabledSignModes: enabledSignModes,
		CustomSignModes:  customSignModes,
	})
	if err != nil {
		panic(err)
	}
	return txConfig
}

// ConfigOptions define the configuration of a TxConfig when calling NewTxConfigWithOptions.
// An empty struct is a valid configuration and will result in a TxConfig with default values.
type ConfigOptions struct {
	// If SigningHandler is specified it will be used instead constructing one.
	// This option supersedes all below options, whose sole purpose are to configure the creation of
	// txsigning.HandlerMap.
	SigningHandler *txsigning.HandlerMap
	// EnabledSignModes is the list of sign modes that will be enabled in the txsigning.HandlerMap.
	EnabledSignModes []signing.SignMode
	// If SigningContext is specified it will be used when constructing sign mode handlers. If nil, one will be created
	// with the options specified in SigningOptions.
	SigningContext *txsigning.Context
	// SigningOptions are the options that will be used when constructing a txsigning.Context and sign mode handlers.
	// If nil defaults will be used.
	SigningOptions *txsigning.Options
	// TextualCoinMetadataQueryFn is the function that will be used to query coin metadata when constructing
	// textual sign mode handler. This is required if SIGN_MODE_TEXTUAL is enabled.
	TextualCoinMetadataQueryFn textual.CoinMetadataQueryFn
	// CustomSignModes are the custom sign modes that will be added to the txsigning.HandlerMap.
	CustomSignModes []txsigning.SignModeHandler
	// ProtoDecoder is the decoder that will be used to decode protobuf transactions.
	ProtoDecoder sdk.TxDecoder
	// ProtoEncoder is the encoder that will be used to encode protobuf transactions.
	ProtoEncoder sdk.TxEncoder
	// JSONDecoder is the decoder that will be used to decode json transactions.
	JSONDecoder sdk.TxDecoder
	// JSONEncoder is the encoder that will be used to encode json transactions.
	JSONEncoder sdk.TxEncoder
}

// NewTxConfigWithOptions returns a new protobuf TxConfig using the provided ProtoCodec, ConfigOptions and
// custom sign mode handlers. If ConfigOptions is an empty struct then default values will be used.
func NewTxConfigWithOptions(protoCodec codec.Codec, configOptions ConfigOptions) (client.TxConfig, error) {
	txConfig := &config{
		protoCodec:  protoCodec,
		decoder:     configOptions.ProtoDecoder,
		encoder:     configOptions.ProtoEncoder,
		jsonDecoder: configOptions.JSONDecoder,
		jsonEncoder: configOptions.JSONEncoder,
	}
	if configOptions.ProtoDecoder == nil {
		txConfig.decoder = DefaultTxDecoder(protoCodec)
	}
	if configOptions.ProtoEncoder == nil {
		txConfig.encoder = DefaultTxEncoder()
	}
	if configOptions.JSONDecoder == nil {
		txConfig.jsonDecoder = DefaultJSONTxDecoder(protoCodec)
	}
	if configOptions.JSONEncoder == nil {
		txConfig.jsonEncoder = DefaultJSONTxEncoder(protoCodec)
	}

	var err error
	if configOptions.SigningContext == nil {
		if configOptions.SigningOptions == nil {
			configOptions.SigningOptions, err = NewDefaultSigningOptions()
			if err != nil {
				return nil, err
			}
		}
		if configOptions.SigningOptions.FileResolver == nil {
			configOptions.SigningOptions.FileResolver = protoCodec.InterfaceRegistry()
		}
		configOptions.SigningContext, err = txsigning.NewContext(*configOptions.SigningOptions)
		if err != nil {
			return nil, err
		}
	}
	txConfig.signingContext = configOptions.SigningContext

	if configOptions.SigningHandler != nil {
		txConfig.handler = configOptions.SigningHandler
		return txConfig, nil
	}

	txConfig.handler, err = NewSigningHandlerMap(configOptions)
	if err != nil {
		return nil, err
	}

	return txConfig, nil
}

// DefaultTxDecoder returns a default protobuf TxDecoder using the provided Marshaler.
func DefaultTxDecoder(cdc codec.Codec) sdk.TxDecoder {
	return func(txBytes []byte) (sdk.Tx, error) {
		// Make sure txBytes follow ADR-027.
		err := rejectNonADR027TxRaw(txBytes)
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		var raw tx.TxRaw

		// reject all unknown proto fields in the root TxRaw
		err = unknownproto.RejectUnknownFieldsStrict(txBytes, &raw, cdc.InterfaceRegistry())
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		err = cdc.Unmarshal(txBytes, &raw)
		if err != nil {
			return nil, err
		}

		var body tx.TxBody

		// allow non-critical unknown fields in TxBody
		txBodyHasUnknownNonCriticals, err := unknownproto.RejectUnknownFields(raw.BodyBytes, &body, true, cdc.InterfaceRegistry())
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		err = cdc.Unmarshal(raw.BodyBytes, &body)
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		var authInfo tx.AuthInfo

		// reject all unknown proto fields in AuthInfo
		err = unknownproto.RejectUnknownFieldsStrict(raw.AuthInfoBytes, &authInfo, cdc.InterfaceRegistry())
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		err = cdc.Unmarshal(raw.AuthInfoBytes, &authInfo)
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		theTx := &tx.Tx{
			Body:       &body,
			AuthInfo:   &authInfo,
			Signatures: raw.Signatures,
		}

		return &wrapper{
			tx:                           theTx,
			bodyBz:                       raw.BodyBytes,
			authInfoBz:                   raw.AuthInfoBytes,
			txBodyHasUnknownNonCriticals: txBodyHasUnknownNonCriticals,
			cdc:                          cdc,
		}, nil
	}
}

// DefaultJSONTxDecoder returns a default protobuf JSON TxDecoder using the provided Marshaler.
func DefaultJSONTxDecoder(cdc codec.Codec) sdk.TxDecoder {
	return func(txBytes []byte) (sdk.Tx, error) {
		var theTx tx.Tx
		err := cdc.UnmarshalJSON(txBytes, &theTx)
		if err != nil {
			return nil, errorsmod.Wrap(sdkerrors.ErrTxDecode, err.Error())
		}

		return &wrapper{
			tx:  &theTx,
			cdc: cdc,
		}, nil
	}
}

// rejectNonADR027TxRaw rejects txBytes that do not follow ADR-027. This is NOT
// a generic ADR-027 checker, it only applies decoding TxRaw. Specifically, it
// only checks that:
// - field numbers are in ascending order (1, 2, and potentially multiple 3s),
// - and varints are as short as possible.
// All other ADR-027 edge cases (e.g. default values) are not applicable with
// TxRaw.
func rejectNonADR027TxRaw(txBytes []byte) error {
	// Make sure all fields are ordered in ascending order with this variable.
	prevTagNum := protowire.Number(0)

	for len(txBytes) > 0 {
		tagNum, wireType, m := protowire.ConsumeTag(txBytes)
		if m < 0 {
			return fmt.Errorf("invalid length; %w", protowire.ParseError(m))
		}
		// TxRaw only has bytes fields.
		if wireType != protowire.BytesType {
			return fmt.Errorf("expected %d wire type, got %d", protowire.BytesType, wireType)
		}
		// Make sure fields are ordered in ascending order.
		if tagNum < prevTagNum {
			return fmt.Errorf("txRaw must follow ADR-027, got tagNum %d after tagNum %d", tagNum, prevTagNum)
		}
		prevTagNum = tagNum

		// All 3 fields of TxRaw have wireType == 2, so their next component
		// is a varint, so we can safely call ConsumeVarint here.
		// Byte structure: <varint of bytes length><bytes sequence>
		// Inner  fields are verified in `DefaultTxDecoder`
		lengthPrefix, m := protowire.ConsumeVarint(txBytes[m:])
		if m < 0 {
			return fmt.Errorf("invalid length; %w", protowire.ParseError(m))
		}
		// We make sure that this varint is as short as possible.
		n := varintMinLength(lengthPrefix)
		if n != m {
			return fmt.Errorf("length prefix varint for tagNum %d is not as short as possible, read %d, only need %d", tagNum, m, n)
		}

		// Skip over the bytes that store fieldNumber and wireType bytes.
		_, _, m = protowire.ConsumeField(txBytes)
		if m < 0 {
			return fmt.Errorf("invalid length; %w", protowire.ParseError(m))
		}
		txBytes = txBytes[m:]
	}

	return nil
}

// varintMinLength returns the minimum number of bytes necessary to encode an
// uint using varint encoding.
func varintMinLength(n uint64) int {
	switch {
	// Note: 1<<N == 2**N.
	case n < 1<<(7):
		return 1
	case n < 1<<(7*2):
		return 2
	case n < 1<<(7*3):
		return 3
	case n < 1<<(7*4):
		return 4
	case n < 1<<(7*5):
		return 5
	case n < 1<<(7*6):
		return 6
	case n < 1<<(7*7):
		return 7
	case n < 1<<(7*8):
		return 8
	case n < 1<<(7*9):
		return 9
	default:
		return 10
	}
}

// DefaultTxEncoder returns a default protobuf TxEncoder using the provided Marshaler
func DefaultTxEncoder() sdk.TxEncoder {
	return func(tx sdk.Tx) ([]byte, error) {
		txWrapper, ok := tx.(*wrapper)
		if !ok {
			return nil, fmt.Errorf("expected %T, got %T", &wrapper{}, tx)
		}

		raw := &txtypes.TxRaw{
			BodyBytes:     txWrapper.getBodyBytes(),
			AuthInfoBytes: txWrapper.getAuthInfoBytes(),
			Signatures:    txWrapper.tx.Signatures,
		}

		return proto.Marshal(raw)
	}
}

// DefaultJSONTxEncoder returns a default protobuf JSON TxEncoder using the provided Marshaler.
func DefaultJSONTxEncoder(cdc codec.Codec) sdk.TxEncoder {
	return func(tx sdk.Tx) ([]byte, error) {
		txWrapper, ok := tx.(*wrapper)
		if ok {
			return cdc.MarshalJSON(txWrapper.tx)
		}

		return nil, fmt.Errorf("expected %T, got %T", &wrapper{}, tx)
	}
}

// NewDefaultSigningOptions returns the sdk default signing options used by x/tx.  This includes account and
// validator address prefix enabled codecs.
func NewDefaultSigningOptions() (*txsigning.Options, error) {
	sdkConfig := sdk.GetConfig()
	return &txsigning.Options{
		AddressCodec:          authcodec.NewBech32Codec(sdkConfig.GetBech32AccountAddrPrefix()),
		ValidatorAddressCodec: authcodec.NewBech32Codec(sdkConfig.GetBech32ValidatorAddrPrefix()),
	}, nil
}

// NewSigningHandlerMap returns a new txsigning.HandlerMap using the provided ConfigOptions.
// It is recommended to use types.InterfaceRegistry in the field ConfigOptions.FileResolver as shown in
// NewTxConfigWithOptions but this fn does not enforce it.
func NewSigningHandlerMap(configOpts ConfigOptions) (*txsigning.HandlerMap, error) {
	var err error
	if configOpts.SigningOptions == nil {
		configOpts.SigningOptions, err = NewDefaultSigningOptions()
		if err != nil {
			return nil, err
		}
	}
	if configOpts.SigningContext == nil {
		configOpts.SigningContext, err = txsigning.NewContext(*configOpts.SigningOptions)
		if err != nil {
			return nil, err
		}
	}

	signingOpts := configOpts.SigningOptions

	if len(configOpts.EnabledSignModes) == 0 {
		configOpts.EnabledSignModes = DefaultSignModes
	}

	lenSignModes := len(configOpts.EnabledSignModes)
	handlers := make([]txsigning.SignModeHandler, lenSignModes+len(configOpts.CustomSignModes))
	for i, m := range configOpts.EnabledSignModes {
		var err error
		switch m {
		case signing.SignMode_SIGN_MODE_DIRECT:
			handlers[i] = &direct.SignModeHandler{}
		case signing.SignMode_SIGN_MODE_DIRECT_AUX:
			handlers[i], err = directaux.NewSignModeHandler(directaux.SignModeHandlerOptions{
				TypeResolver:   signingOpts.TypeResolver,
				SignersContext: configOpts.SigningContext,
			})
			if err != nil {
				return nil, err
			}
		case signing.SignMode_SIGN_MODE_LEGACY_AMINO_JSON:
			handlers[i] = aminojson.NewSignModeHandler(aminojson.SignModeHandlerOptions{
				FileResolver: signingOpts.FileResolver,
				TypeResolver: signingOpts.TypeResolver,
			})
		case signing.SignMode_SIGN_MODE_TEXTUAL:
			handlers[i], err = textual.NewSignModeHandler(textual.SignModeOptions{
				CoinMetadataQuerier: configOpts.TextualCoinMetadataQueryFn,
				FileResolver:        signingOpts.FileResolver,
				TypeResolver:        signingOpts.TypeResolver,
			})
			if configOpts.TextualCoinMetadataQueryFn == nil {
				return nil, fmt.Errorf("cannot enable SIGN_MODE_TEXTUAL without a TextualCoinMetadataQueryFn")
			}
			if err != nil {
				return nil, err
			}
		}
	}
	for i, m := range configOpts.CustomSignModes {
		handlers[i+lenSignModes] = m
	}

	handler := txsigning.NewHandlerMap(handlers...)
	return handler, nil
}

// DefaultSignModes are the default sign modes enabled for protobuf transactions.
var DefaultSignModes = []signing.SignMode{
	signing.SignMode_SIGN_MODE_DIRECT,
	signing.SignMode_SIGN_MODE_DIRECT_AUX,
	signing.SignMode_SIGN_MODE_LEGACY_AMINO_JSON,
	// signingtypes.SignMode_SIGN_MODE_TEXTUAL is not enabled by default, as it requires a x/bank keeper or gRPC connection.
}
