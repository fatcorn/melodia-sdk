package nsblockstore

type VMNamespaceBLockMaker interface {
	HasNameBLock() bool

	MakeBlock(blockArgs ...interface{}) *NamespaceBLock
}
