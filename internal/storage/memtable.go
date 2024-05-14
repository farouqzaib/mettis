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
		inMemoryVectorIndex:   index.NewHNSW(5, 0.62, 2, 16),
		sizeLimit:             sizeLimit,
		logger:                logger,
	}

	return m
}

func (m *Memtable) HasRoomForWrite(data []byte) bool {
	invertedIndexBytes, err := m.inMemoryInvertedIndex.Encode()

	if err != nil {
		panic(err)
	}

	hnswBytes, err := m.inMemoryVectorIndex.Encode()

	if err != nil {
		panic(err)
	}

	sizeNeeded := len(invertedIndexBytes) + len(hnswBytes) + len(data)
	sizeAvailable := m.sizeLimit - m.sizeUsed

	return sizeNeeded <= sizeAvailable
}

func (m *Memtable) Index(docID int, document string) error {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex, m.logger, index.GetEmbedding)
	err := h.Index(docID, document)

	if err != nil {
		return err
	}

	m.sizeUsed = len([]byte(document))

	return nil
}

func (m *Memtable) BulkIndex(docIDs []float64, documents []string) error {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex, m.logger, index.GetEmbedding)
	err := h.BulkIndex(docIDs, documents)

	if err != nil {
		return err
	}

	l := 0
	for _, document := range documents {
		l += len([]byte(document))
	}
	m.sizeUsed = l

	return nil
}

func (m *Memtable) Get(query string, k int) ([]index.Match, error) {
	h := index.NewHybridSearch(m.inMemoryInvertedIndex, m.inMemoryVectorIndex, m.logger, index.GetEmbedding)

	matches, err := h.Search(query, k)

	if err != nil {
		return []index.Match{}, err
	}

	return matches, nil
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}
