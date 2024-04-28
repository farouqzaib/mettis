package storage

import (
	"bufio"
	"compress/gzip"
	"io"

	"github.com/farouqzaib/fast-search/internal/index"
)

type Reader struct {
	file io.Closer
	br   *bufio.Reader
}

func NewReader(file io.Reader) *Reader {
	r := &Reader{}
	r.file, _ = file.(io.Closer)
	r.br = bufio.NewReader(file)
	return r
}

func (r *Reader) loadInvertedIndex() (index.InvertedIndex, error) {
	reader, err := gzip.NewReader(r.br)
	if err != nil {
		if err == io.EOF {
			panic(err)
		}
		panic(err)
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		if err == io.EOF {
			panic(err)
		}
		panic(err)
	}

	var i index.InvertedIndex

	i = i.Decode(b)

	return i, nil
}

func (r *Reader) loadVectorIndex() (index.HNSW, error) {
	reader, err := gzip.NewReader(r.br)
	if err != nil {
		if err == io.EOF {
			panic(err)
		}
		panic(err)
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		if err == io.EOF {
			panic(err)
		}
		panic(err)
	}

	var i index.HNSW

	i = i.Decode(b)

	return i, nil
}

func (r *Reader) Close() error {
	err := r.file.Close()
	if err != nil {
		return err
	}

	r.file = nil
	r.br = nil
	return nil
}
