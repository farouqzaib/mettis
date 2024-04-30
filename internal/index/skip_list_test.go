package index

import (
	"testing"
)

func TestSkipListSearch(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 3})
	skipList.Insert(Position{DocumentID: 2, Offset: 9})
	skipList.Insert(Position{DocumentID: 3, Offset: 1})
	skipList.Insert(Position{DocumentID: 4, Offset: 30})
	skipList.Insert(Position{DocumentID: 5, Offset: 13})

	got, err := skipList.Find(Position{DocumentID: 1, Offset: 3})

	if err != nil {
		t.Fatalf("expected document offset, got %v", got)
	}

}

func TestSkipListLast(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 3})
	skipList.Insert(Position{DocumentID: 2, Offset: 9})
	skipList.Insert(Position{DocumentID: 3, Offset: 1})
	skipList.Insert(Position{DocumentID: 4, Offset: 30})
	skipList.Insert(Position{DocumentID: 5, Offset: 13})

	expected := Position{DocumentID: 5, Offset: 13}

	got := skipList.Last()

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindLessThan(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 2})
	skipList.Insert(Position{DocumentID: 3, Offset: 3})

	expected := Position{DocumentID: 1, Offset: 2}

	got, _ := skipList.FindLessThan(Position{DocumentID: 1, Offset: 3})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindLessThanWhenOffsetPresent(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 3})
	skipList.Insert(Position{DocumentID: 2, Offset: 9})
	skipList.Insert(Position{DocumentID: 3, Offset: 1})
	skipList.Insert(Position{DocumentID: 3, Offset: 10})
	skipList.Insert(Position{DocumentID: 4, Offset: 30})
	skipList.Insert(Position{DocumentID: 5, Offset: 13})

	expected := Position{DocumentID: 3, Offset: 1}
	key := Position{DocumentID: 3, Offset: 10}
	got, _ := skipList.FindLessThan(key)

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindGreaterThan(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 1})
	skipList.Insert(Position{DocumentID: 1, Offset: 2})
	skipList.Insert(Position{DocumentID: 3, Offset: 3})

	expected := Position{DocumentID: 1, Offset: 2}

	key := Position{DocumentID: 1, Offset: 1}

	got, _ := skipList.FindGreaterThan(key)

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindGreaterThanWhenOffsetPresent(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 3})
	skipList.Insert(Position{DocumentID: 2, Offset: 9})
	skipList.Insert(Position{DocumentID: 3, Offset: 1})
	skipList.Insert(Position{DocumentID: 3, Offset: 2})
	skipList.Insert(Position{DocumentID: 4, Offset: 30})
	skipList.Insert(Position{DocumentID: 5, Offset: 13})

	expected := Position{DocumentID: 4, Offset: 30}

	got, _ := skipList.FindGreaterThan(Position{DocumentID: 4, Offset: 2})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindGreaterThanOneRecord(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 3})

	expected := Position{DocumentID: EOF, Offset: EOF}

	got, _ := skipList.FindGreaterThan(Position{DocumentID: 3, Offset: 1})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}

func TestSkipListFindLessThanOneRecord(t *testing.T) {
	skipList := NewSkipList()

	skipList.Insert(Position{DocumentID: 1, Offset: 3})

	expected := Position{DocumentID: BOF, Offset: BOF}

	got, _ := skipList.FindLessThan(Position{DocumentID: 1, Offset: 1})

	if expected.DocumentID != got.DocumentID && expected.Offset != got.Offset {
		t.Fatalf("expected %v, got %v", expected, got)
	}

}
