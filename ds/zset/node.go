package zset

// SortedSetLevel records forward and span
type SortedSetLevel struct {
	forward *SortedSetNode
	span int64
}

// SortedSetNode represents a node in the SortedSet
type SortedSetNode struct {
	key 		string		//unique key of this node
	Value 		[]byte		// associated data
	score 		SCORE		// score to determine the order of this node in the set
	backward 	*SortedSetNode
	level		[]SortedSetLevel
}

// Key returns the key of the node
func (ssn *SortedSetNode) Key() string {
	return ssn.key
}

// Score returns the score of the node
func (ssn *SortedSetNode) Score() SCORE {
	return ssn.score
}