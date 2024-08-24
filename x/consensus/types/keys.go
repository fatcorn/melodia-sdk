package types

const (
	// ModuleName defines the name of the x/consensus module.
	ModuleName = "consensus"

	// StoreKey defines the module's store key.
	StoreKey = ModuleName
)

type (
	SortMode string
)

const (
	SortModeGasPrice SortMode = "gas"
	SortModeQueue    SortMode = "queue"
)

func CheckSortMode(sortMode SortMode) bool {
	switch sortMode {
	case SortModeGasPrice:
		return true
	case SortModeQueue:
		return true
	}
	return false
}
func GetAllSortModeToString() string {
	str := SortModeGasPrice + "," + SortModeQueue
	return string(str)
}
