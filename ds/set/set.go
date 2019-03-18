package set

import "errors"

// Set represents the Set
type Set struct {
	M map[string]map[string]struct{}
}

// New returns a newly initialized Set object that implements the Set
func New() *Set {
	return &Set {
		M: make(map[string]map[string]struct{}),
	}
}

// SAdd adds the specified members to the set stored at key
func (s *Set) SAdd(key string, items ...[]byte) error {
	if _, ok := s.M[key]; !ok {
		s.M[key] = make(map[string]struct{})
	}

	for _, item := range items {
		s.M[key][string(item)] = struct{}{}
	}

	return nil
}

// SRem removes the specified members from the set stored at key
func (s *Set) SRem(key string, items ...[]byte) error {
	if _, ok := s.M[key]; !ok {
		return errors.New("key not found")
	}

	if len(items) == 0 {
		return errors.New("item empty")
	}

	for _, item := range items {
		delete(s.M[key], string(item))
	}

	return nil
}

// SHasKey returns if has the set at given key
func (s *Set) SHasKey (key string) bool {
	if _ , ok := s.M[key]; !ok {
		return false
	}

	return true
}


// SPop removes and returns one or more random elements from the set value store at key
func (s *Set) SPop(key string) []byte {
	if !s.SHasKey(key) {
		return nil
	}

	for item := range s.M[key] {
		delete(s.M[key], item)
		return []byte(item)
	}

	return nil
}

// SCard returns the set cardinality (number of elements) of the set stored at key
func (s *Set) SCard(key string) int {
	if !s.SHasKey(key) {
		return 0
	}

	return len(s.M[key])
}

// SDiff returns the members of the set resulting from the difference between the first set and all the successive set
func (s *Set) Sdiff(key1, key2 string) (list [][]byte, err error) {
	if _, err = s.checkKey1AndKey2(key1, key2); err != nil {
		return
	}

	for item1 := range s.M[key1] {
		if _, ok := s.M[key2][item1]; !ok {
			list = append(list, []byte(item1))
		}
	}

	return
}

// SInter returns the members of the set resulting from the intersection of all the given sets
func (s *Set) SInter(key1, key2 string) (list [][]byte, err error) {
	if _, err = s.checkKey1AndKey2(key1, key2); err != nil {
		return
	}

	for item1 := range s.M[key1] {
		if _, ok := s.M[key1][item1]; ok {
			list = append(list, []byte(item1))
		}
	}

	return
}

// checkkey1AndKey2 return if key1 and key2 exists
func (s *Set) checkKey1AndKey2(key1, key2 string) (list [][]byte, err error) {
	if _, ok := s.M[key1]; !ok {
		return nil, errors.New("set1 is not exists")
	}

	if _, ok := s.M[key2]; !ok {
		return nil, errors.New("set2 is not exists")
	}

	return nil, nil
}

// SIsMember returns if member is a member of the set stored at key
func (s *Set) SIsMember(key string, item []byte) bool {
	if _, ok := s.M[key]; !ok {
		return false
	}

	if _, ok := s.M[key][string(item)]; !ok{
		return false
	}

	return true
}

// SAreMenber returns if members are members of the set stored at key
// For multiple items it returns true only if all of the items exist
func (s *Set) SAreMember(key string, items ...[]byte) (bool, error) {
	if _, ok := s.M[key]; !ok {
		return false, errors.New("key not exists")
	}

	for _,  item := range items {
		if _, ok := s.M[key][string(item)]; !ok {
			return false, errors.New("item not exists")
		}
	}

	return true, nil
}

// SMembers returns all the members of the set value stored at key
func (s *Set) SMembers(key string) (list [][]byte, err error) {
	if _, ok := s.M[key]; !ok {
		return nil, errors.New("set not exists")
	}

	for item := range s.M[key] {
		list = append(list, []byte(item))
	}

	return
}

// SMove moves member from the set at source to the set at destination
func (s *Set) SMove(key1, key2 string, item []byte) (bool, error) {
	if !s.SHasKey(key1) {
		return false, errors.New("key1 is not exists")
	}

	if !s.SHasKey(key2) {
		return false, errors.New("key2 is not exists")
	}

	if _, ok := s.M[key2][string(item)]; !ok {
		s.SAdd(key2, item)
	}

	s.SRem(key1, item)
	return true, nil
}

// SUnion returns the members of the set resulting from the union of all the given sets
func (s *Set) SUnion(key1, key2 string) (list [][]byte, err error) {
	if _, err = s.checkKey1AndKey2(key1, key2); err != nil {
		return
	}

	for item1 := range s.M[key1] {
		list = append(list, []byte(item1))
	}

	for item2 := range s.M[key2] {
		if _, ok := s.M[key1][key2]; !ok {
			list = append(list, []byte(item2))
		}
	}

	return
}