package storage

import "github.com/farouqzaib/fast-search/internal/index"

// Many thanks: https://www.cloudcentric.dev/exploring-memtables/

type Memtable struct {
	inMemoryInvertedIndex *index.InvertedIndex
	inMemoryVectorIndex   *index.HNSW
	sizeUsed              int
	sizeLimit             int
}

func NewMemtable(sizeLimit int) *Memtable {
	m := &Memtable{
		inMemoryInvertedIndex: index.NewInvertedIndex(),
		inMemoryVectorIndex:   index.NewHNSW(5, 0.62, 10),
		sizeLimit:             sizeLimit,
	}

	return m
}

func (m *Memtable) HasRoomForWrite(data []byte) bool {
	l := len(m.inMemoryInvertedIndex.Encode())
	l += len(m.inMemoryVectorIndex.Encode())

	sizeNeeded := l + len(data)
	sizeAvailable := m.sizeLimit - m.sizeUsed

	return sizeNeeded <= sizeAvailable
}

func (m *Memtable) Insert(docID int, document string) {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex)
	err := h.Index(docID, document)

	if err != nil {
		panic(err)
	}

	l := len(m.inMemoryInvertedIndex.Encode())
	l += len(m.inMemoryVectorIndex.Encode())

	m.sizeUsed = l
}

func (m *Memtable) Get(query string, k int) []index.Match {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex)

	return h.Search(query, k)
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}