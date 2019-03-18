package zset

import "math/rand"

conset (
	// SkipListMaxLevel represents the skipList max level number
	SkipListMaxLevel = 32

	// SkipListP represents the p parameter of the skipList
	SkipListP = 0.25
)

// SCORE represents the score type
type SCORE float64

// SortedSet represents the sorted set
type SortedSet struct {
	header 	*SortedSetNode
	tail 	*SortedSetNode
	length 	int64
	level	int
	Dict 	map[string]*SortedSetNode
}

// randomLevel returns a random level to the new skiplist node we are going to create
// The return value of this function is between 1 and SkipListMaxLevel
// (both inclusive), with a powerlaw-alike distribution where higher
// levels are less likely to be returned
func randomLevel() int {
	level := 1
	for float64(rand.Int31()&0xFFFF) < float64(SkipListP*0xFFFF) {
		level += 1
	}

	if level < SkipListMaxLevel {
		return level
	}

	return SkipListMaxLevel
}