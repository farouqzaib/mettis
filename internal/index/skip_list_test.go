package index

import (
	"log/slog"
	"testing"
)

func TestSkipListSearch(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 3})
	skipList.Insert(DocumentOffset{DocumentID: 2, Offset: 9})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 1})
	skipList.Insert(DocumentOffset{DocumentID: 4, Offset: 30})
	skipList.Insert(DocumentOffset{DocumentID: 5, Offset: 13})

	got, err := skipList.Find(DocumentOffset{DocumentID: 1, Offset: 3})

	if err != nil {
		t.Fatalf("expected document offset, got %v", got)
	}

}

func TestSkipListLast(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 3})
	skipList.Insert(DocumentOffset{DocumentID: 2, Offset: 9})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 1})
	skipList.Insert(DocumentOffset{DocumentID: 4, Offset: 30})
	skipList.Insert(DocumentOffset{DocumentID: 5, Offset: 13})

	expected := DocumentOffset{DocumentID: 5, Offset: 13}

	got := skipList.Last()

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindLessThan(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 2})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 3})

	expected := DocumentOffset{DocumentID: 1, Offset: 2}

	got, _ := skipList.FindLessThan(DocumentOffset{DocumentID: 1, Offset: 3})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindLessThanWhenOffsetPresent(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 3})
	skipList.Insert(DocumentOffset{DocumentID: 2, Offset: 9})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 1})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 10})
	skipList.Insert(DocumentOffset{DocumentID: 4, Offset: 30})
	skipList.Insert(DocumentOffset{DocumentID: 5, Offset: 13})

	expected := DocumentOffset{DocumentID: 3, Offset: 1}
	key := DocumentOffset{DocumentID: 3, Offset: 10}
	got, _ := skipList.FindLessThan(key)

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindGreaterThan(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 1})
	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 2})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 3})

	expected := DocumentOffset{DocumentID: 1, Offset: 2}

	key := DocumentOffset{DocumentID: 1, Offset: 1}

	got, _ := skipList.FindGreaterThan(key)

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindGreaterThanWhenOffsetPresent(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 3})
	skipList.Insert(DocumentOffset{DocumentID: 2, Offset: 9})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 1})
	skipList.Insert(DocumentOffset{DocumentID: 3, Offset: 2})
	skipList.Insert(DocumentOffset{DocumentID: 4, Offset: 30})
	skipList.Insert(DocumentOffset{DocumentID: 5, Offset: 13})

	expected := DocumentOffset{DocumentID: 4, Offset: 30}

	got, _ := skipList.FindGreaterThan(DocumentOffset{DocumentID: 4, Offset: 2})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindGreaterThanOneRecord(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 3})

	expected := DocumentOffset{DocumentID: EOF, Offset: EOF}

	got, _ := skipList.FindGreaterThan(DocumentOffset{DocumentID: 3, Offset: 1})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindLessThanOneRecord(t *testing.T) {
	skipList := NewSkipList(slog.Default())

	skipList.Insert(DocumentOffset{DocumentID: 1, Offset: 3})

	expected := DocumentOffset{DocumentID: BOF, Offset: BOF}

	got, _ := skipList.FindLessThan(DocumentOffset{DocumentID: 1, Offset: 1})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}
