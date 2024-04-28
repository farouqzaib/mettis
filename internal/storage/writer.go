package storage

import (
	"bufio"
	"compress/gzip"
	"io"
	"log"
)

const bufLimit = 100000000

type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file syncCloser
	bw   *bufio.Writer
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw

	return w
}

func (w *Writer) WriteDataBlock(inMemoryIndex []byte) error {
	gz, err := gzip.NewWriterLevel(w.bw, 9)
	if err != nil {
		panic(err)
	}
	if _, err := gz.Write(inMemoryIndex); err != nil {
		log.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (w *Writer) Close() error {
	err := w.bw.Flush()
	if err != nil {
		return err
	}

	err = w.file.Sync()
	if err != nil {
		return err
	}

	err = w.file.Close()
	if err != nil {
		return err
	}

	w.bw = nil
	w.file = nil
	return err
}
