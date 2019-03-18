package nutsdb

// EntryIdxMode represents entry index mode
type EntryIdxMode int

const (
	// HintKeyValAndRAMIdxMode represents ram index (key and value) mode
	HintKeyValAndRAMIdxMode EntryIdxMode = iota

	// HintKeyAndRAMIdxMode represents ram index (only key) mode
	HintKeyANDRAMIdxMode
)

// Optinos records params for creating DB object
type Options struct {
	// Dir represents Open the database located in whick Dir
	Dir string

	// EntryIdxMode represents using which mode to index the entries
	EntryIdxMode EntryIdxMode

	// RWMode represents the read and write mode
	// RWMode includes two options: FileIO and MMap
	// FilIO represents the read and write mode using standard I/O
	// MMap represents the read and write mode using mmap
	RWMode		RWMode
	SegmentSize int64

	// NodeNum represents the node number
	// Default NodeNum is 1. NodeNum range [1,1023]
	NodeNum int64

	// SyncEnable represents if call Sync() function
	// if SyncEnable is false, high write performance but potential data loss likely
	// is SyncEnable is true, slower but persistent
	SyncEnable bool

	// StartFileLoadingMode represents when open a database which RWMode to load files
	StartFileLoadingMode RWMode
}

var defaultSegmenSize int64 = 8 * 1024 * 1024

// DefaultOptions represents the default options
var DefaultOptions = Options{
	EntryIdxMode:			HintKeyValAndRAMIdxMode,
	SegmentSize:			defaultSegmenSize,
	NodeNum:				1,
	RWMode:					FileIO,
	SyncEnable:				true,
	StartFileLoadingMode:	MMap,
}