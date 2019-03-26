package nutsdb

import (
	"bytes"
	"errors"
)

var (
	// ErrStartKey is return when Rnage is called by a error start key
	ErrStartKey = errors.New("err start key")

	// ErrScansNoResult is returned when Range or prefixScan are called no result to found
	ErrScanNoResult = errors.New("range scans or prefix scans no result")

	// ErrPrefixScanNoResult is returned when prefixScan is called no result to found
	ErrPrefixScansNoResult = errors.New("prefix scans no result")

	// ErrKeyNotFound is returned when the key is not in the b+ tree
	ErrKeyNotFound = errors.New("key not found ")
)

const (
	// Default number of b+ tree orders
	order = 8

	// RangeScan returns range scanMode flag
	RangScan = "RangeScan"

	// PrefixScan returns prefix scanMode flag
	CountFlagEnabled = true

	// CountFlagDisabled returns disabled CountFlag
	CountFlagDisabled = false
)

type (
	// BPTree records toot node and valid key number
	BPTree struct {
		root 			*Node
		ValidKeyCount	int // the number of the key that not expired or deleted
		idxType			int
	}

	// Records records multi-records as result when is called Range or PrefixScan
	Records map[string]*Records

	// Node records keys and pointers and parent node
	Node struct {
		Keys		[][]byte
		pointers	[]interface{}
		parent		*Node
		isLeaf		bool
		KeysNum		int
	}
)

// newNode returns a newly initialized Node object that implements the Node
func newNode() *Node {
	return &Node{
		Keys:		make([][]byte, order-1),
		pointers:	make([]interface{}, order),
		isLeaf:		false,
		parent:		nil,
		KeysNum:	0,
	}
}

// newLeaf returns a newly initialized Node object that implements the Node and set isLeaf flag
func newLeaf() *Node{
	leaf := newNode()
	leaf.isLeaf = true
	return leaf
}

// NewTree returns a newly initialized BPTree Object that implements the BPTree
func NewTree() *BPTree {
	return &BPTree{}
}

// FindLeaf returns leaf at the given key
func (t *BPTree) FindLeaf(key []byte) *Node {
	var (
		i		int
		curr	*Node
	)

	if curr = t.root; curr == nil {
		return nil
	}

	for !curr.isLeaf {
		i = 0
		for i < curr.KeysNum {
			if compare(key, curr.Keys[i]) >= 0 {
				i++
			} else {
					break
			}
		}

		curr = curr.pointers[i].(*Node)
	}

	return curr
}

// Compare returns an integer comparing two byte slices lexicographically
// The result will be 0 if a=b, -1 if a < b, and +1 if a > b
// A nil argument is equivalent to an empty slice
func compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// findRange returns numFound. keys and pointers at the given start key and key
func (t *BPTree) findRange(start, end []byte) (numFound int, keys [][]byte, pointers []interface{}) {
	var (
		n			*Node
		i,j			int
		scanFlag	bool
	)

	if n = t.FindLeaf(start); n == nil {
		return 0, nil, nil
	}

	for j = 0; i < n.KeysNum && compare(n.Keys[j], start) < 0 {
		j++
	}

	scanFlag = true
	for n != nil && scanFlag {
		for i = j; i < n.KeysNum; i ++ {
			if compare(n.Keys[i], end) > 0 {
				scanFlag = false
				break
			}

			keys = append(keys, n.Keys[i])
			pointers = append(pointers, n.pointers[i])
			numFound++
		}

		n, _ = n.pointers[order-1].(*Node)
		j = 0
	}

	return
}

// Range returns records at the given start key and end key
func (t *BPTree) Range(start, end []byte) (records Records, err error) {
	if compare(start, end) > 0 {
		return nil, ErrStartKey
	}

	return getRecordWrapper(t.findRange(start, end))
}

// getRecordWrapper returns a wrapper of records when Range or PrefixScan are called
func getRecordWrapper(numFound int, keys [][]byte, pointers []interface{}) (records Records, err error) {
	if numFound == 0 {
		return nil, ErrScanNoResult
	}

	records = make(Records)
	for i := 0; i < numFound; i++ {
		records[string(keys[i])] = pointers[i].(*Record)
	}

	return records, nil
}

// PrefixScan returns records at the given prefix and limitNum
// limitNum: limit the number of the scanned records return
func (t *BPTree) PrefixScan(prefix []byte, limitNum int) (records Records, err error) {
	var (
		n				*Node
		scanFlag		bool
		keys			[][]byte
		pointers		[]interface{}
		i, j, numberFound	int
	)

	n = t.FindLeaf(prefix)

	if n == nil {
		return nil, ErrPrefixScansNoResult
	}

	for j = 0; j < n.KeysNum && compare(n.Keys[j], prefix) < 0; {
		j++
	}

	scanFlag = true
	numberFound = 0
	for n != nil && scanFlag {
		for i = j; i < n.KeysNum; i++ {
			if !bytes.HasPrefix(n.Keys[i], prefix) {
				scanFlag = false
				break
			}

			keys = append(keys, n.Keys[i])
			pointers = append(pointers, n.pointers[i])
			numberFound++

			if limitNum > 0 && numberFound == limitNum {
				scanFlag = false
				break
			}
		}

		n, _ = n.pointers[order-1].(*Node)
		j = 0
	}

	return getRecordWrapper(numberFound, keys, pointers)
}

// Find retrieves record at the given key
func (t *BPTree) Find(key []byte) (*Record, error) {
	var (
		leaf 	*Node
		i		int
	)

	// Find leaf by key
	leaf = t.FindLeaf(key)

	if leaf == nil {
		return nil, ErrKeyNotFound
	}

	for i = 0; i < leaf.KeysNum; i++ {
		if compare(key, leaf.Keys[i]) == 0 {
			break
		}
	}

	if i == leaf.KeysNum {
		return nil, ErrKeyNotFound
	}

	return leaf.pointers[i].(*Record), nil
}

// startNewTree returns a start new tree
func (t *BPTree) startNewTree(key []byte, pointer *Record) error {
	t.root = newLeaf()
	t.root.Keys[0] = key
	t.root.pointers[0] = pointer
	t.root.KeysNum = 1

	return nil
}

// Insert inserts record to the b+ tree
// and if the key exists, update the record and the counter(if countFlag set true, it will start count)
func (t *BPTree)Insert(key []byte, e *Entry, h *Hint, countFlag bool) error {
	if r, err := t.Find(key); err == nil && r != nil {
		if countFlag && h.meta.Flag == DataDeleteFlag && r.H.meta.Flag != DataDeleteFlag && t.ValidKeyCount > 0 {
			t.ValidKeyCount--
		}

		if countFlag && h.meta.Flag != DataDeleteFlag && r.H.meta.Flag == DataDeleteFlag {
			t.ValidKeyCount++
		}

		return r.UpdateRecord(h, e)
	}

	// Initialize the Record object when key does not exist
	pointer := &Record{H:h, E:e}

	// UPdate the validKeyCount number
	t.ValidKeyCount++

	// Check if the root nodes is nil or not
	// if nil build a start new tree for insert
	if t.root == nil {
		return t.startNewTree(key, pointer)
	}

	// Find the leaf node to insert
	leaf := t.FindLeaf(key)

	// Check if the leaf node is full or not
	// if not full insert into the leaf node
	if leaf.KeysNum < order-1 {
		insertIntoLeaf(leaf, key, pointer)
		return nil
	}

	// split the leaf node when it is not enough space to insert
	return t.splitLeaf(leaf, key, pointer)
}

// splitLeaf splits leaf and insert the parent node when the leaf is full
func (t *BPTree) splitLeaf(leaf *Node, key []byte, pointer *Record) eror {
	var j, k, i int

	tempKeys := make([][]byte, order)
	tempPointers := make([]interface{}, order)


}

// insertIntoNewRoot returns a now root when the insertIntoParent is called
func (t *BPTree) insertIntoNewRoot(left *Node, key []byte, right *Node) error {
	t.root = newNode()

	t.root.Keys[0] = key
	t.root.pointers[0] = left
	t.root.pointers[1] = right
	t.root.KeysNum++
	t.root.parent = nil

	left.parent = t.root
	right.parent = t.root

	return nil
}

// insertIntoLeaf inserts the given node at the given key and pointer
func insertIntoLeaf(leaf *Node, key []byte, pointer *Record) {
	i := 0
	for i < leaf.KeysNum {
		if compare(key, leaf.Keys[i]) > 0 {
			i++
		} else {
				break
		}
	}

	for j := leaf.KeysNum; j > i; j-- {
		leaf.Keys[j] = leaf.Keys[j-1]
		leaf.pointers[j] = leaf.pointers[j-1]
	}

	leaf.Keys[i] = key
	leaf.pointers[i] = pointer
	leaf.KeysNum++
}