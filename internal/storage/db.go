package storage

import (
	"errors"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"sort"

	"github.com/farouqzaib/fast-search/internal/index"
)

const (
	memtableSizeLimit        = 20000000
	memtableFlushThreshold   = bufLimit
	VectorIndexSegmentPath   = "vectorindex"
	InvertedIndexSegmentPath = "invertedindex"
	DocumentMetadataBucket   = "documentbucket"
)

type IndexStorage struct {
	dataStorage *Provider
	memtables   struct {
		mutable *Memtable
		queue   []*Memtable
	}
	segments                   []*FileMetadata
	invertedIndexSegmentReader []*os.File
	vectorIndexSegmentReader   []*os.File
	inMemorySegments           []index.InvertedIndex
	inMemoryVectorSegments     []index.HNSW
	logger                     *slog.Logger
}

func Open(dirname string, logger *slog.Logger) (*IndexStorage, error) {
	dataStorage, err := NewProvider(dirname)
	if err != nil {
		return nil, err
	}

	db := &IndexStorage{dataStorage: dataStorage, logger: logger}
	err = db.loadSegments()
	if err != nil {
		return nil, err
	}
	db.memtables.mutable = NewMemtable(memtableSizeLimit, logger)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)

	return db, nil
}

func (d *IndexStorage) Index(docID int, document string) error {
	l := len(d.memtables.mutable.inMemoryInvertedIndex.Encode())
	l += len(d.memtables.mutable.inMemoryVectorIndex.Encode())

	needed := []byte(document)
	if l+len(needed) > memtableFlushThreshold {
		return errors.New("file too large to be indexed")
	}

	m := d.memtables.mutable

	if !m.HasRoomForWrite(needed) {
		m = d.rotateMemtables()
	}

	m.Insert(docID, document)

	d.maybeScheduleFlush()

	if d.memtables.mutable.sizeUsed > memtableFlushThreshold {
		//drop the memtable if size is too large to fit buffer
		if len(d.memtables.queue) > 1 {
			d.memtables.queue = d.memtables.queue[:len(d.memtables.queue)-2]
		} else {
			d.memtables.queue = d.memtables.queue[:len(d.memtables.queue)-1]
		}

		d.memtables.mutable = NewMemtable(memtableSizeLimit, d.logger)
		d.memtables.queue = append(d.memtables.queue, d.memtables.mutable)
	}

	return nil
}

func (d *IndexStorage) rotateMemtables() *Memtable {
	d.memtables.mutable = NewMemtable(memtableSizeLimit, d.logger)
	d.memtables.queue = append(d.memtables.queue, d.memtables.mutable)
	return d.memtables.mutable
}

func (d *IndexStorage) Get(query string, k int) []index.Match {
	matches := []index.Match{}
	matchesCh := make(chan []index.Match, len(d.segments))

	for i := len(d.memtables.queue) - 1; i >= 0; i-- {
		m := d.memtables.queue[i]

		val := m.Get(query, k)

		matches = append(matches, val...)
	}

	for j := len(d.segments) - 1; j >= 0; j-- {
		go func(j int) {

			h := index.NewHybridSearch(&d.inMemorySegments[j], &d.inMemoryVectorSegments[j], d.logger)

			val := h.Search(query, k)
			matchesCh <- val
		}(j)
	}

	for j := len(d.segments) - 1; j >= 0; j-- {
		r := <-matchesCh
		matches = append(matches, r...)
	}

	sort.Slice(matches, func(i, j int) bool {
		return matches[i].Score > matches[j].Score
	})

	k = int(math.Min(float64(k), float64(len(matches))))
	return matches[:k]
}

func (d *IndexStorage) maybeScheduleFlush() {
	var totalSize int

	for i := 0; i < len(d.memtables.queue); i++ {
		totalSize += d.memtables.queue[i].Size()
	}

	if totalSize <= memtableFlushThreshold {
		return
	}

	slog.Info("total size to flush", slog.Int("size", totalSize))
	err := d.FlushMemtables()
	if err != nil {
		log.Fatal(err)
	}
}

func (d *IndexStorage) FlushMemtables() error {
	slog.Info("flushing memtables")
	n := len(d.memtables.queue) - 1

	if len(d.memtables.queue) == 1 {
		n = len(d.memtables.queue)
	}

	flushable := d.memtables.queue[:n]
	d.memtables.queue = d.memtables.queue[n:]

	for i := 0; i < len(flushable); i++ {
		meta := d.dataStorage.PrepareNewFile()

		err := d.writeSegment(flushable[i].inMemoryInvertedIndex.Encode(), meta, InvertedIndexSegmentPath)
		if err != nil {
			return err
		}
		err = d.writeSegment(flushable[i].inMemoryVectorIndex.Encode(), meta, VectorIndexSegmentPath)
		if err != nil {
			return err
		}

		d.segments = append(d.segments, meta)
	}
	return nil
}

func (d *IndexStorage) Reader() []io.Reader {
	invertedIndexReaders := make([]io.Reader, len(d.invertedIndexSegmentReader))
	vectorIndexReaders := make([]io.Reader, len(d.vectorIndexSegmentReader))
	for i, reader := range d.invertedIndexSegmentReader {
		invertedIndexReaders = append(invertedIndexReaders, reader)
		vectorIndexReaders = append(vectorIndexReaders, d.vectorIndexSegmentReader[i])
	}

	return []io.Reader{io.MultiReader(invertedIndexReaders...), io.MultiReader(vectorIndexReaders...)}
}

func (d *IndexStorage) loadSegments() error {
	slog.Info("loading segments")
	meta, err := d.dataStorage.ListFiles()
	if err != nil {
		return err
	}

	for _, f := range meta {
		if !f.IsSegment() {
			continue
		}

		reader, err := d.dataStorage.OpenFileForReading(f, InvertedIndexSegmentPath)
		if err != nil {
			return err
		}
		r := NewReader(reader)

		d.segments = append(d.segments, f)
		d.dataStorage.fileNum = f.fileNum
		d.invertedIndexSegmentReader = append(d.invertedIndexSegmentReader, reader)

		invertedIndex, err := r.loadInvertedIndex()
		if err != nil {
			return err
		}
		d.inMemorySegments = append(d.inMemorySegments, invertedIndex)

		reader, err = d.dataStorage.OpenFileForReading(f, VectorIndexSegmentPath)
		if err != nil {
			return err
		}
		r = NewReader(reader)

		d.vectorIndexSegmentReader = append(d.vectorIndexSegmentReader, reader)

		vectorIndex, err := r.loadVectorIndex()
		if err != nil {
			return err
		}

		d.inMemoryVectorSegments = append(d.inMemoryVectorSegments, vectorIndex)
	}

	return nil
}

func (d *IndexStorage) writeSegment(b []byte, meta *FileMetadata, indexType string) error {
	f, err := d.dataStorage.OpenFileForWriting(meta, indexType)
	if err != nil {
		return err
	}

	w := NewWriter(f)
	err = w.WriteDataBlock(b)
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}

	return err
}
