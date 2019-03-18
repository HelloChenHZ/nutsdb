package nutsdb

import "errors"

var (
	// ErrKeyAndValSize is returned when given key and value size is too big
	ErrKeyAndValSize = errors.New("key and value size too big")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back
	ErrTxClosed = errors.New("tx is closed")

	// ErrTxNoWritable is returned when performing a write operation on
	// a read-only transaction
	ErrTxNoWritable = errors.New("tx not writable")

	// ErrKeyEmpty is returned if any empty key is passed on an update function
	ErrKeyEmpty = errors.New("key cannot be empty")

	// ErrRangeScan is returned when range sacnning not found the result
	ErrPrefixScan = errors.New("prefix scans not found'")

	// ErrNotFoundKey is returned when key not found in the bucket on an view function
	ErrNotFoundKey = errors.New("key not found in the bucket")
)

type Tx struct {
	id			uint64
	db			*DB

}