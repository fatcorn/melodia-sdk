package types

import "fmt"

var (
	NamespaceKeyPrefix = "namesapce_"
)

func getNamespaceIdKey(tokenSourceNamespaceId uint64) string {
	return fmt.Sprintf("%s%v", NamespaceKeyPrefix, tokenSourceNamespaceId)
}

func GetDenomPrefix(tokenSourceNamespaceId uint64) string {
	return fmt.Sprintf("%s/%s/", ModuleName, getNamespaceIdKey(tokenSourceNamespaceId))
}

// GetPrefixedDenom returns the denomination with the portID and channelID prefixed
func GetPrefixedDenom(sourceNamespaceId uint64, baseDenom string) string {
	return fmt.Sprintf("%s/%s/%s", ModuleName, getNamespaceIdKey(sourceNamespaceId), baseDenom)
}
