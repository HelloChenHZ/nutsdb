package nutsdb

import "github.com/pkg/errors"

var (
	// ErrCrcZero is returned when crc is 0
	ErrCrcZero = errors.New("error crc is 0")

	// ErrCrc is returned when crc is error
	ErrCrc = errors.New("crc error")

	// ErrCapacity is returned when capacity is error
	ErrCapacity = errors.New("capacity error")
)

const (
	// DataSuffix return the data suffix
	DataSuffix = ".dat"

	// DataEntryHeaderSize returns the entry header size
	DataEntryHeaderSize = 42
)

// DataFile records about data file inframation
type DataFile struct {
	path 		string
	fileID 		int64
	writeOff 	int64
	ActualSize	int64
	rwManager 	RWManager
}

// NewDataFile returns a newly initialized DataFile object
func NewDataFile(path string, capacity int64, rwMode RWMode) (df *DataFile, err error) {
	var rwManager RWManager

	if capacity <= 0 {
		return nil, ErrCapacity
	}

	if rwMode == FileIO {
		rwManager, err = NewFileIORWManager(path, capacity)
		if err != nil {
			return nil, err
		}
	}

	if rwMode = MMap {
		rwManager, err = NewMMapRWManager(path, capacity)
		if err != nil {
			return nil, err
		}
	}

	return &DataFile{
		path:		path,
		writeOff:	0,
		ActualSize: 0,
		rwManager: rwManager,
	}, nil
}