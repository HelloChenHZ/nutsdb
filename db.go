package nutsdb

import (
	"errors"
	"fmt"
	"github.com/HelloChenHZ/nutsdb/ds/list"
	"github.com/HelloChenHZ/nutsdb/ds/set"
	"github.com/HelloChenHZ/nutsdb/ds/zset"
	"github.com/xujiajun/utils/filesystem"
	"github.com/xujiajun/utils/strconv2"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
)

var (
	// ErrDBClosed is returned when db is closed
	ErrDBClosed = errors.New("db is closed")

	// ErrBucket is returned when bucket is not in the HintIdx
	ErrBucket = errors.New("err bucket")

	// ErrEntryIdxModeOpt is returned when set db EntryIdxMode option is wriong
	ErrEntryIdxModeOpt = errors.New("err EntryIdxMode option set")

	// ErrFn is returned when fn is nil
	ErrFn = errors.New("err fn")
)

const (
	// DataDeleteFlag represents the data delete flag
	DataDeleteFlag uint16 = iota

	// DataSetFlag represents the data set flag
	DataSetFlag

	// DataLPushFlag represents the data LPush flag
	DataLPushFlag

	// DataRPushFlag represents the data RPush flag
	DataRPushFlag

	// DataLRemFlag represents the data LRem falg
	DataLRemFlag

	// DataLPopFlag represents the data LPop flag
	DataLPopFlag

	// DataRPopFlag represents the data RPop flag
	DataRPopFlag

	// DataLSetFlag represents the data LSet flag
	DataLSetFlag

	// DataLTrimFlag represents the data LTrim flag
	DataLTrimFlag

	// DataZAddFlag represents the data ZAdd flag
	DataZAddFlag

	// DataZRemFlag represents the data ZRem flag
	DataZRemFlag

	// DataZRemRangeByRankFlag represents the data ZRemRangeByRank flag
	DataZRemRangeByRankFlag

	// DataZPopMaxFlag represents the data ZPopMax flag
	DataZPopMaxFlag

	// DataZPopMinFlag represents the data aZPopMin flag
	DataZPopMinFlag
)


const (
	// UnCommitted represents the tx unCommitted status
	UnCommitted uint16 = 0

	// Committed represents the tx committed status
	Committed uint16 = 1

	// Presistent represents the data persistent flag
	Persistent uint32 = 0

	// ScanNoLimit represents the data scan no limit flag
	ScanNoLimit int = -1
)

const (
	// DataStructSet represents the data structure set flag
	DataStructureSet uint16 = iota

	// DataStructureSortedSet represents the data structure storted set flag
	DataStructureSortedSet

	// DataStructureBPTree represents the data structure b+ tree flag
	DataStrucctureBPTree

	// DataStructureList represents the data structure list flag
	DataStructureList
)

type (
	DB struct {
		opt 			Options		// the database options
		BPTreeIdx 		BPTreeIdx	// Hint Index
		SetIdx 			SetIdx
		SortedSetIdx	SortedSetIdx
		ListIdx 		ListIdx
		ActiveFile		*DataFile
		MaxFileID		int64
		mu 				sync.RWMutex
		KeyCount		int // total key number, include expired, deleted, repeated
		closed			bool
		isMergeing		bool
		committedTxIds	map[uint64]struct{}
	}

	// BPTreeIdx represents the B+ tree index
	BPTreeIdx map[string]*BPTree

	// SetIdx represents the sorted set index
	SetIdx map[string]*set.Set

	// SortedSetIdx represents the sorted set index
	SortedSetIdx map[string]*zset.SortedSetNode

	// ListIdx represents the list index
	ListIdx map[string]*list.List

	// Entries represents entry map
	Entries map[string]*Entry
)

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

	return &DataFile {
		path:		path,
		writeOff:	0,
		ActualSize:	0,
		rwManager:	rwManager,
	}, nil
}

// Open returns a newly initialized DB object
func Open(opt Options) (*DB, error) {
	db := &DB{
		BPTreeIdx: 		make(BPTreeIdx),
		SetIdx:			make(SetIdx),
		SortedSetIdx: 	make(SortedSetIdx),
		ListIdx:		make(ListIdx),
		MaxFileID:		0,
		opt:			opt,
		KeyCount: 		0,
		closed:			false,
		committedTxIds:	make(map[uint64]struct{}),
	}

	if ok := filesystem.PathIsExist(db.opt.Dir); !ok {
		if err := os.MkdirAll(db.opt.Dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	if err := db.buildIndexes(); err != nil {
		return nil, fmt.Errorf("db.buildIdexes error: %s", err)
	}

	return db, nil
}

// setActiveFile sets the ActiveFile (DataFile object)
func (db *DB) setActiveFile() (err error) {
	filepath := db.getDataPath(db.MaxFileID)
	db.ActiveFile, err = NewDataFIle(filepath, db.opt.SegmentSize, db.opt.RWMode)
}

// getMaxFileIDAndFileds returns max fileId and fileIds
func (db *DB) getMaxFileIDAndFileIDs() (maxFileID int64, dataFileIds []int) {
	files, _ := ioutil.ReadDir(db.opt.Dir)
	if len(files) == 0 {
		return 0, nil
	}

	maxFileID = 0

	for _, f := range files {
		id := f.Name()
		fileSuffix := path.Ext(path.Base(id))
		if fileSuffix != DataSuffix {
			continue
		}

		id = strings.TrimSuffix(id, DataSuffix)
		idVal, _ := strconv2.StrToInt(id)
		dataFileIds = append(dataFileIds, idVal)
	}

	sort.Ints(dataFileIds)
	maxFileID = int64(dataFileIds[len(dataFileIds)-1])

	return
}

// getActiveFileWriteOff returns the write offset of activeFile
func (db *DB) getActiveFileWriteOff() (off int64, err error) {
	off  = 0
	for {
		if item, err := db.ActiveFile.ReadAt(int(off)); err == nil {
			if item == nil {
				break
			}

			off += item.Size()
			//set ActiveFileActualSize
			db.ActiveFile.ActualSize = off
		} else {
			if err == io.EOF {
				break
			}

			return -1, fmt.Errorf("when build activeDataIndex readAt err: %s", err)
		}
	}

	return
}

// getDataPath returns the data path at given fid
func (db *DB) getDataPath(fID int64) string {
	return db.opt.Dir + "/" + strconv2.Int64ToStr(fID) + DataSuffix
}

// buildIndexes builds indexes when db initialize resource
func (db *DB) buildIndexes() (err error) {
	var (
		maxFileID	int64
		dataFileIds	[]int
	)

	maxFileID, dataFileIds = db.getMaxFileIDAndFileIDs()

	//init db.ActiveFile
	db.MaxFileID = maxFileID

	//set ActiveFile
	if err = db.setActiveFile(); err != nil {
		return
	}

	if dataFileIds == nil && maxFileID == 0 {
		return
	}

	if db.ActiveFile.writeOff, err = db.getActiveFileWriteOff(); err != nil {
		return
	}

	// build hint index
	return db.buildHintIdx(dataFileIds)
}

// buildHintIdx builds the Hint Indexes
func (db *DB) buildHintIdx(dataFileIds []int) error {
	unconfirmedRecords, committedTxIds, err := db.parseDataFiles(dataFileIds)
}