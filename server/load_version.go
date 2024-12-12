package server

import (
	"encoding/hex"
	"os"

	"github.com/spf13/cobra"

	"github.com/cosmos/cosmos-sdk/client/flags"
	"github.com/cosmos/cosmos-sdk/server/types"
)

const ()

// LoadVersionCmd load height status
func LoadVersionCmd(loadVersion types.LoadVersion, defaultNodeHome string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "load-version",
		Short: "Load state id",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			serverCtx := GetServerContextFromCmd(cmd)
			config := serverCtx.Config

			homeDir, _ := cmd.Flags().GetString(flags.FlagHome)
			config.SetRoot(homeDir)

			if _, err := os.Stat(config.GenesisFile()); os.IsNotExist(err) {
				return err
			}

			db, err := openDB(config.RootDir, GetAppDBBackend(serverCtx.Viper))
			if err != nil {
				return err
			}
			traceWriterFile, _ := cmd.Flags().GetString(flagTraceStore)
			traceWriter, err := openTraceWriter(traceWriterFile)
			if err != nil {
				return err
			}

			height, _ := cmd.Flags().GetInt64(FlagHeight)

			versionCommitId, err := loadVersion(serverCtx.Logger, db, traceWriter, height, serverCtx.Viper)
			println("Load version ", "version ", versionCommitId.Version, "hash", hex.EncodeToString(versionCommitId.Hash))
			return nil
		},
	}

	cmd.Flags().String(flags.FlagHome, defaultNodeHome, "The application home directory")
	cmd.Flags().Int64(FlagHeight, -1, "Load state from a particular height (-1 means latest height)")

	return cmd
}
