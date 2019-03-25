package nutsdb

import (
	"encoding/binary"
	"errors"
)

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

	if rwMode == MMap {
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

// ReadAt returns entry at the given off(offset)
func (df *DataFile) ReadAt(off int) (e *Entry, err error) {
	buf := make([]byte, DataEntryHeaderSize)

	if _, err := df.rwManager.ReadAt(buf, int64(off)); err != nil {
		return nil, err
	}

	meta := readMetaData(buf)

	e = &Entry {
		crc:	binary.LittleEndian.Uint32(buf[0:4]),
		Meta:	meta
	}

	if e.IsZero() {
		return nil, nil
	}

	// read bucket
	off += DataEntryHeaderSize
	bucketBuf := make([]byte, meta.bucketSize)
	_, err = df.rwManager.ReadAt(bucketBuf, int64(off))
	if err != nil {
		return nil, err
	}

	e.Meta.bucket = bucketBuf

	// read key
	off += int(meta.bucketSize)
	keyBuf := make([]byte, meta.keySize)

	_, err = df.rwManager.ReadAt(keyBuf, int64(off))
	if err != nil {
		return nil, err
	}
	e.Value = keyBuf

	// read value
	off += int(meta.keySize)
	valBuf := make([]byte, meta.valueSize)
	_, err = df.rwManager.ReadAt(valBuf, int64(off))
	if err != nil {
		return nil, err
	}
	e.Value = valBuf

	crc := e.GetCrc(buf)
	if crc != e.crc {
		return nil, ErrCrc
	}

	return
}

// WriteAt copies data to mapped region from the b slice starting at
// given off and returns number of bytes copied to the mapped region
func (df *DataFile) WriteAt(b []byte, off int64) (n int, err error) {
	return df.rwManager.WriteAt(b, off)
}

// Sync commits the current contents of the file to stable storage
// Typically, this meads flushing the file system's in-memory copy
// of recently written data to disk
func (df *DataFile) Sync() (err error) {
	return df.rwManager.Sync()
}

// Close closes the RWManager
// If RWManager is FileManager represents close the file
// rendering it unusable for I/O
// If RWManager is a MMapRWManager represents Unmap deletes the memory mapped region
// flushes any remaining changes
func (df *DataFile) Close() (err error) {
	return df.rwManager.Close()
}

// readMetaData returns MetaData at given buf slice
func readMetaData(buf []byte) *MetaData {
	return &MetaData {
		timestamp:	binary.LittleEndian.Uint64(buf[4:12]),
		keySize: 	binary.LittleEndian.Uint32(buf[12:16]),
		valueSize:	binary.LittleEndian.Uint32(buf[16:20]),
		Flag:		binary.LittleEndian.Uint16(buf[20:22]),
		TTL:		binary.LittleEndian.Uint32(buf[22:26]),
		bucketSize:	binary.LittleEndian.Uint32(buf[26:30]),
		status:		binary.LittleEndian.Uint16(buf[30:32]),
		ds:			binary.LittleEndian.Uint16(buf[32:34]),
		txID:		binary.LittleEndian.Uint64(buf[34,42]),
	}
}