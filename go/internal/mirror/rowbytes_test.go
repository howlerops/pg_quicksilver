package mirror

import "testing"

// The row-group byte cap counts what a reader decodes: the text and bytes of a
// row, and eight bytes for each other value.
func TestRowBytesCountsTextAndBytesAndFixedValues(t *testing.T) {
	row := []any{int64(1), "abcde", []byte{1, 2, 3}, nil, 2.5}
	want := int64(8 + 5 + 3 + 8 + 8)
	if got := rowBytes(row); got != want {
		t.Errorf("rowBytes = %d, want %d", got, want)
	}
	if got := rowBytes(nil); got != 0 {
		t.Errorf("rowBytes of no values = %d, want 0", got)
	}
}
