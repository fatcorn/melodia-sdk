package cli

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/cosmos/cosmos-sdk/client"
	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/client/tx"
	"github.com/cosmos/cosmos-sdk/version"
	"github.com/spf13/cobra"

	"github.com/cosmos/cosmos-sdk/x/pibc/types"
)

const (
	flagPacketTimeoutHeight    = "packet-timeout-height"
	flagPacketTimeoutTimestamp = "packet-timeout-timestamp"
	flagAbsoluteTimeouts       = "absolute-timeouts"
	flagMemo                   = "memo"
)

// NewOnRecvPacketCmd invoke PIBC OnRecvPacket method
func NewOnRecvPacketCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "pibc-packet-recv [related-tx]",
		Short:   "invoke onRecvPacket method of related tx",
		Long:    strings.TrimSpace(`Invoke onRecvPacket method of related tx`),
		Example: fmt.Sprintf("%s tx pibc-packet-recv [amount]", version.AppName),
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientCtx, err := client.GetClientTxContext(cmd)
			queryClient := types.NewQueryClient(clientCtx)
			if err != nil {
				return err
			}
			//sender := clientCtx.GetFromAddress().String()

			relatedTxId, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}

			rep, err := queryClient.SourcePacket(clientCtx.CmdContext, &types.QuerySourcePacketRequest{TxId: relatedTxId})
			if err != nil {
				return err
			}
			basePacket := rep.BasePacket

			msg := types.NewMsgRecvPacket(
				basePacket.SourceNamespaceId,
				basePacket.TargetNamespaceId,
				basePacket.Data,
				relatedTxId,
			)
			msg.Sender = clientCtx.FromAddress.String()
			return tx.GenerateOrBroadcastTxCLI(clientCtx, cmd.Flags(), msg)
		},
	}

	flags.AddTxFlagsToCmd(cmd)

	return cmd
}

// NewAcknowledgementCmd invoke PIBC OnRecvPacket method
func NewAcknowledgementCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "pibc-ack [related-tx-hash]",
		Short:   "invoke Acknowledgement method of related tx, to rollback the related tx",
		Long:    strings.TrimSpace(`Invoke Acknowledgement method of related tx`),
		Example: fmt.Sprintf("%s tx pibc-ack [related-tx-hash]", version.AppName),
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			clientCtx, err := client.GetClientTxContext(cmd)
			if err != nil {
				return err
			}
			queryClient := types.NewQueryClient(clientCtx)
			if err != nil {
				return err
			}
			//sender := clientCtx.GetFromAddress().String()

			relatedTxId, err := hex.DecodeString(args[0])
			if err != nil {
				return err
			}

			rep, err := queryClient.SourcePacket(clientCtx.CmdContext, &types.QuerySourcePacketRequest{TxId: relatedTxId})
			if err != nil {
				return err
			}
			basePacket := rep.BasePacket

			//if !strings.HasPrefix(coin.Denom, "pibc/") {
			//	denomTrace := types.ParseDenomTrace(coin.Denom)
			//	coin.Denom = denomTrace.IBCDenom()
			//}

			msg := types.NewMsgAcknowledgement(
				basePacket.SourceNamespaceId,
				basePacket.TargetNamespaceId,
				basePacket.Data,
				relatedTxId,
			)
			msg.Sender = clientCtx.FromAddress.String()
			return tx.GenerateOrBroadcastTxCLI(clientCtx, cmd.Flags(), msg)
		},
	}

	flags.AddTxFlagsToCmd(cmd)

	return cmd
}
