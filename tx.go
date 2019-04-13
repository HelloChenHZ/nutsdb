package nutsdb

import (
	"errors"
	"github.com/bwmarrin/snowflake"
)

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

// Tx represents a transaction
type Tx struct {
	id				uint64
	db				*DB
	writable		bool
	pendingWrites	[]*Entry
}

// Begin opens a new transaction
// Mutiple read-only transactions can be opened at the same time but there can
// only be one read/write transaction at a time. Attempting to open a read/write
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed
// All transactions must be closed by calling Commit() or Rollback() when done
func (db *DB) Begin(writable bool) (tx *Tx. err error) {
	tx, err = newTx(db, writable)
	if err != nil {
		return nil, err
	}

	tx.lock()

	if db.closed {
		tx.unlock()
		return nil, ErrDBClosed
	}

	return
}

// newTx returns a newly initialized Tx object at given writable
func newTx(db *DB, writable bool) (tx *Tx, err error) {
	var txID uint64

	tx = &Tx{
		db:				db,
		writable:		writable,
		pendingWrites:	[]*Entry{},
	}

	txID, err = tx.getTxID()
	if err != nil {
		return nil, err
	}

	tx.id = txID
	return
}

// getTxID returns the tx id
func (tx *Tx) getTxID() (id uint64, err error) {
	node, err := snowflake.NewNode(tx.db.opt.NodeNum)
	if err != nil {
		return 0, err
	}

	id = uint64(node.Generate().Int64())
	return
}

// lock locks the database based on the transaction type
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}