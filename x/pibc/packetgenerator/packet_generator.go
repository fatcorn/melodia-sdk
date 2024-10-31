package packetgenerator

import (
	"fmt"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/pibc/keeper"
	"github.com/cosmos/cosmos-sdk/x/pibc/types"
)

// NewPacketGeneratorHandler returns an AnteHandler that checks and increments sequence
// numbers, checks signatures & account numbers, and deducts fees from the first
// signer.
func NewPacketGeneratorHandler(pibcKeeper keeper.Keeper) *sdk.PIBCPacketGeneratorHandler {

	pibcApps := pibcKeeper.Router.ListRoutes()

	return sdk.NewPIBCPacketGeneratorHandler(pibcApps, PIBCTxChecker())

}

func PIBCTxChecker() sdk.PIBCTxChecker {
	return func(ctx sdk.Context, tx sdk.Tx) (sdk.IncludePIBCMsg, error) {
		crossNamespaceTxCount := 0
		for _, msg := range tx.GetMsgs() {
			if _, ok := msg.(types.CrossNamespaceTx); ok {
				crossNamespaceTxCount++
			}
		}
		if crossNamespaceTxCount > 1 {
			return true, fmt.Errorf("PIBC tx only allow one pibc CrossNamespaceTx msg")
		}
		return crossNamespaceTxCount != 0, nil
	}
}
