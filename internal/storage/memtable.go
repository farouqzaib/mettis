package storage

import (
	"log/slog"

	"github.com/farouqzaib/fast-search/internal/index"
)

// Many thanks: https://www.cloudcentric.dev/exploring-memtables/

type Memtable struct {
	inMemoryInvertedIndex *index.InvertedIndex
	inMemoryVectorIndex   *index.HNSW
	sizeUsed              int
	sizeLimit             int
	logger                *slog.Logger
}

func NewMemtable(sizeLimit int, logger *slog.Logger) *Memtable {
	m := &Memtable{
		inMemoryInvertedIndex: index.NewInvertedIndex(),
		inMemoryVectorIndex:   index.NewHNSW(5, 0.62, 8, 16),
		sizeLimit:             sizeLimit,
		logger:                logger,
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

func (m *Memtable) Index(docID int, document string) {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex, m.logger, index.GetEmbedding)
	err := h.Index(docID, document)

	if err != nil {
		panic(err)
	}

	m.sizeUsed = len([]byte(document))
}

func (m *Memtable) BulkIndex(docIDs []float64, documents []string) {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex, m.logger, index.GetEmbedding)
	err := h.BulkIndex(docIDs, documents)

	if err != nil {
		panic(err)
	}

	l := 0
	for _, document := range documents {
		l += len([]byte(document))
	}
	m.sizeUsed = l
}

func (m *Memtable) Get(query string, k int) []index.Match {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex, m.logger, index.GetEmbedding)

	return h.Search(query, k)
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}
