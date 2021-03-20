package storage

// WalManagerInterface should be implemented
// by core implementation of WalManager
type WalManagerInterface interface {
	Run(int) error
	Record([]byte) (RecordHandle, error)
	Load(int) ([]byte, error)
}
