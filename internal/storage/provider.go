package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type Provider struct {
	dataDir string
	fileNum int
}

type FileType int

const (
	FileTypeUknown FileType = iota
	FileTypeSegment
)

type FileMetadata struct {
	fileNum  int
	fileType FileType
}

func (f *FileMetadata) IsSegment() bool {
	return f.fileType == FileTypeSegment
}

func (f *FileMetadata) FileNum() int {
	return f.fileNum
}

func NewProvider(dataDir string) (*Provider, error) {
	s := &Provider{dataDir: dataDir}

	err := s.ensureDataDirExists()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Provider) ensureDataDirExists() error {
	err := os.MkdirAll(filepath.Join(s.dataDir, InvertedIndexSegmentPath), 0755)
	if err != nil {
		return err
	}

	err = os.MkdirAll(filepath.Join(s.dataDir, VectorIndexSegmentPath), 0755)
	if err != nil {
		return err
	}
	return nil
}

func (s *Provider) ListFiles() ([]*FileMetadata, error) {
	files, err := os.ReadDir(filepath.Join(s.dataDir, InvertedIndexSegmentPath))
	if err != nil {
		return nil, err
	}

	var meta []*FileMetadata
	var fileNumber int
	var fileExtension string
	for _, f := range files {
		if f.Name() == ".DS_Store" {
			continue
		}
		_, err = fmt.Sscanf(f.Name(), "%06d.%s", &fileNumber, &fileExtension)

		if err != nil {
			return nil, err
		}

		fileType := FileTypeUknown
		if fileExtension == "segment" {
			fileType = FileTypeSegment
		}

		meta = append(meta, &FileMetadata{
			fileNum:  fileNumber,
			fileType: fileType,
		})
	}

	return meta, nil
}

func (s *Provider) nextFileNum() int {
	s.fileNum++
	return s.fileNum
}

func (s *Provider) generateFileName(fileNumber int) string {
	return fmt.Sprintf("%06d.segment", fileNumber)
}

func (s *Provider) PrepareNewFile() *FileMetadata {
	return &FileMetadata{
		fileNum:  s.nextFileNum(),
		fileType: FileTypeSegment,
	}
}

func (s *Provider) OpenFileForWriting(meta *FileMetadata, indexType string) (*os.File, error) {
	const openFlags = os.O_RDWR | os.O_CREATE | os.O_EXCL
	filename := s.generateFileName(meta.fileNum)
	file, err := os.OpenFile(filepath.Join(s.dataDir, indexType, filename), openFlags, 0644)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func (s *Provider) OpenFileForReading(meta *FileMetadata, indexType string) (*os.File, error) {
	const openFlags = os.O_RDONLY
	filename := s.generateFileName(meta.fileNum)
	file, err := os.OpenFile(filepath.Join(s.dataDir, indexType, filename), openFlags, 0)

	if err != nil {
		return nil, err
	}

	return file, err
}
