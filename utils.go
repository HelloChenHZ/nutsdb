package nutsdb

import (
	"os"
	"sort"
)

// SortedEntryKeys returns sorted entries
func SortedEntryKeys(m map[string]*Entry) (keys []string, es map[string]*Entry) {
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys, m
}

// Truncate change the size of the file
func Truncate(path string, capacity int64, f *os.File) error {
	fileInfo, _ := os.Stat(path)
	if fileInfo.Size() < capacity {
		if err := f.Truncate(capacity); err != nil {
			return err
		}
	}

	return nil
}