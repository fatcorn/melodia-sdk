package transfer

import (
	autocliv1 "cosmossdk.io/api/cosmos/autocli/v1"
	pibctransferv1 "github.com/cosmos/cosmos-sdk/x/pibc/api/pibc/apps/transfer/v1"
)

// AutoCLIOptions implements the autocli.HasAutoCLIConfig interface.
func (am AppModule) AutoCLIOptions() *autocliv1.ModuleOptions {
	return &autocliv1.ModuleOptions{
		//Query: &autocliv1.ServiceCommandDescriptor{
		//	Service: pibctransferv1.Query_ServiceDesc.ServiceName,
		//	RpcCommandOptions: []*autocliv1.RpcCommandOptions{
		//		{
		//			RpcMethod: "Params",
		//			Use:       "params",
		//			Short:     "Shows the parameters of the module",
		//		},
		//		// this line is used by ignite scaffolding # autocli/query
		//	},
		//},
		Tx: &autocliv1.ServiceCommandDescriptor{
			Service:              pibctransferv1.Msg_ServiceDesc.ServiceName,
			EnhanceCustomCommand: true, // only required if you want to use the custom command
			RpcCommandOptions: []*autocliv1.RpcCommandOptions{
				{
					RpcMethod: "Transfer",
					Skip:      true, // skipped because authority gated
				},
				// this line is used by ignite scaffolding # autocli/tx
			},
		},
	}
}
