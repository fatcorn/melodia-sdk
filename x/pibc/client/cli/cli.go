package cli

import (
	"github.com/cosmos/cosmos-sdk/client"
	"github.com/spf13/cobra"
)

// GetQueryCmd returns the query commands for IBC connections
func GetQueryCmd() *cobra.Command {
	queryCmd := &cobra.Command{
		Use:                        "pibc-transfer",
		Short:                      "PIBC fungible token transfer query subcommands",
		DisableFlagParsing:         true,
		SuggestionsMinimumDistance: 2,
	}

	queryCmd.AddCommand(
	//GetCmdQueryDenomTrace(),
	//GetCmdQueryDenomTraces(),
	//GetCmdParams(),
	//GetCmdQueryEscrowAddress(),
	//GetCmdQueryDenomHash(),
	//GetCmdQueryTotalEscrowForDenom(),
	)

	return queryCmd
}

// NewTxCmd returns the transaction commands for IBC fungible token transfer
func NewTxCmd() *cobra.Command {
	txCmd := &cobra.Command{
		Use:                        "pibc",
		Short:                      "PIBC transaction subcommands",
		DisableFlagParsing:         true,
		SuggestionsMinimumDistance: 2,
		RunE:                       client.ValidateCmd,
	}

	txCmd.AddCommand(
		NewOnRecvPacketCmd(),
		NewAcknowledgementCmd(),
	)

	return txCmd
}
