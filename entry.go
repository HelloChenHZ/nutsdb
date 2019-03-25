package nutsdb

type (
	//Entry represents the data item
	Entry struct {
		Key			[]byte
		Value		[]byte
		Meta		*MetaData
		crc			uint32
		position	uint64
	}

	// Hint represents the index of the key
	Hint struct {
		key		[]byte
		fileID	int64
		meta	*MetaData
		dataPos	uint64
	}

	// MetaData represents the meta information of the data item
	MetaData struct {
		keySize		uint32
		valueSize	uint32
		timestamp	uint64
		TTL			uint32
		Flag		uint16 // delete set
		bucket		[]byte
		bucketSize	uint32
		txID		uint64
		status		uint16 // committed / uncommitted
		ds 			uint16 // data structure
	}
)

// Size returns the size of the entry
func (e *Entry) Size() int64 {
	return int64(DataEntryHeaderSize + e.Meta.keySize + e.Meta.valueSize + e.Meta.bucketSize)
}

// Encode returns the slice after the entry be encoded
