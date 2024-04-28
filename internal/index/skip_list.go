package index

import (
	"errors"
	"math"
	"math/rand"
	"time"
)

const (
	MaxHeight = 32
)

var EOF = math.Inf(1)
var BOF = math.Inf(-1)

type DocumentOffset struct {
	DocumentID float64
	Offset     float64
}

func (d *DocumentOffset) GetDocumentID() int {
	return int(d.DocumentID)
}

func (d *DocumentOffset) GetOffset() int {
	return int(d.Offset)
}

var BOFDocument = DocumentOffset{DocumentID: BOF, Offset: BOF}
var EOFDocument = DocumentOffset{DocumentID: EOF, Offset: EOF}

type Node struct {
	Key   DocumentOffset
	Tower [MaxHeight]*Node
}

type SkipList struct {
	Head   *Node
	Height int
}

func NewSkipList() *SkipList {
	return &SkipList{
		Head:   &Node{},
		Height: 1,
	}
}

func (s *SkipList) Search(key DocumentOffset) (*Node, [MaxHeight]*Node) {
	var next *Node
	var journey [MaxHeight]*Node

	prev := s.Head

	for level := s.Height - 1; level >= 0; level-- {
		for next = prev.Tower[level]; next != nil; next = next.Tower[level] {
			if key.DocumentID < next.Key.DocumentID && key.Offset < next.Key.Offset {
				break
			}

			if key.DocumentID < next.Key.DocumentID && key.Offset > next.Key.Offset {
				break
			}

			if key.DocumentID < next.Key.DocumentID && key.Offset == next.Key.Offset {
				break
			}

			if key.DocumentID == next.Key.DocumentID && key.Offset < next.Key.Offset {
				break
			}

			if key.DocumentID == next.Key.DocumentID && key.Offset == next.Key.Offset {
				break
			}

			if key.DocumentID == next.Key.DocumentID && key.Offset > next.Key.Offset {
				//advance
				//modify the lowest level of journey prematurely?
				// level = 0
				// journey[0] = next
			}

			if key.DocumentID > next.Key.DocumentID && key.Offset < next.Key.Offset {
				//advance
			}

			if key.DocumentID > next.Key.DocumentID && key.Offset > next.Key.Offset {
				//advance
			}

			if key.DocumentID > next.Key.DocumentID && key.Offset == next.Key.Offset {
				//advance
			}

			prev = next
		}

		journey[level] = prev
	}

	if next != nil && key.DocumentID == next.Key.DocumentID && key.Offset == next.Key.Offset {
		return next, journey
	}

	return nil, journey
}

func (s *SkipList) Find(key DocumentOffset) (DocumentOffset, error) {
	found, _ := s.Search(key)

	if found == nil {
		return DocumentOffset{DocumentID: EOF, Offset: EOF}, errors.New("key not found")
	}
	return found.Key, nil
}

func (s *SkipList) FindLessThan(key DocumentOffset) (DocumentOffset, error) {
	_, journey := s.Search(key)

	if journey[0] == nil {
		return DocumentOffset{DocumentID: BOF, Offset: BOF}, errors.New("key not found")
	}

	if journey[0] == s.Head {
		return DocumentOffset{DocumentID: BOF, Offset: BOF}, errors.New("no element found")
	}

	return journey[0].Key, nil
}

func (s *SkipList) FindGreaterThan(key DocumentOffset) (DocumentOffset, error) {
	found, journey := s.Search(key)

	//if the key exists then move the found key forward
	if found != nil {
		if found.Tower[0] != nil {
			return found.Tower[0].Key, nil
		} else {
			return DocumentOffset{DocumentID: EOF, Offset: EOF}, errors.New("no element found")
		}
	}

	//maybe check for head first?
	// if journey[0] == s.Head {
	// 	return DocumentOffset{DocumentID: EOF, Offset: EOF}, errors.New("no element found")
	// }

	//move the previous key forward since key does not exist
	if journey[0] != nil && journey[0].Tower[0] != nil {
		//move until you find element greater than key
		return journey[0].Tower[0].Key, nil
	}

	return DocumentOffset{DocumentID: EOF, Offset: EOF}, errors.New("no element found")
}

func (s *SkipList) Insert(key DocumentOffset) {
	found, journey := s.Search(key)

	if found != nil {
		found.Key = DocumentOffset{DocumentID: key.DocumentID, Offset: key.Offset}
		return
	}

	height := s.randomHeight()
	node := &Node{Key: DocumentOffset{DocumentID: key.DocumentID, Offset: key.Offset}}

	for level := 0; level < height; level++ {
		prev := journey[level]

		if prev == nil {
			prev = s.Head
		}

		node.Tower[level] = prev.Tower[level]
		prev.Tower[level] = node
	}

	if height > s.Height {
		s.Height = height
	}

}

func (s *SkipList) Delete(key DocumentOffset) bool {
	found, journey := s.Search(key)

	if found != nil {
		found.Key = DocumentOffset{DocumentID: -1, Offset: -1}
		return false
	}

	for level := 0; level < s.Height; level++ {
		if journey[level].Tower[level] != found {
			break
		}

		journey[level].Tower[level] = found.Tower[level]
		found.Tower[level] = nil
	}

	found = nil
	s.Shrink()
	return true
}

func (s *SkipList) Last() DocumentOffset {
	var next *Node
	prev := s.Head

	for next = prev.Tower[0]; next != nil; next = next.Tower[0] {
		prev = next
	}

	return prev.Key
}

func (s *SkipList) Shrink() {
	for level := s.Height - 1; level >= 0; level-- {
		if s.Head.Tower[level] == nil {
			s.Height--
		}
	}
}

func (s *SkipList) randomHeight() int {
	l := 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for r.Float64() < 0.5 && l < MaxHeight {
		l++
	}
	return l
}

type Iterator struct {
	current *Node
}

func (s *SkipList) Iterator() *Iterator {
	return &Iterator{s.Head.Tower[0]}
}

func (i *Iterator) HasNext() bool {
	return i.current.Tower[0] != nil
}

func (i *Iterator) Next() DocumentOffset {
	i.current = i.current.Tower[0]

	if i.current == nil {
		return DocumentOffset{DocumentID: EOF, Offset: EOF}
	}

	return i.current.Key
}
