package multiple

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index/single"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestReadWrite(t *testing.T) {
	type test struct {
		name    string
		prepare func(indexDir *IndexDirectory[uint32])
		assert  func(indexDir *IndexDirectory[uint32], t *testing.T)
	}

	tests := []test{
		{
			name:    "read from empty folder",
			prepare: func(indexDir *IndexDirectory[uint32]) {},
			assert: func(indexDir *IndexDirectory[uint32], t *testing.T) {
				r, err := indexDir.NewReader()
				require.NoError(t, err)

				// read terms
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)

				terms := go_iterators.ToSlice(termsIterator)
				require.Empty(t, terms)

				// read values
				valuesIterator, err := r.ReadValues([]string{"term1", "term2"}, 0, 100)
				require.NoError(t, err)

				values := go_iterators.ToSlice(valuesIterator)
				require.Empty(t, values)
			},
		},
		{
			name: "write and read terms and values",
			prepare: func(indexDir *IndexDirectory[uint32]) {
				w, err := indexDir.NewWriter()
				require.NoError(t, err)

				err = w.(single.InvertedIndexWriter[uint32]).Put("term1", []uint32{1, 2})
				require.NoError(t, err)
				err = w.(single.InvertedIndexWriter[uint32]).Put("term2", []uint32{2, 3})
				require.NoError(t, err)
				err = w.(single.InvertedIndexWriter[uint32]).Close()
				require.NoError(t, err)
			},
			assert: func(indexDir *IndexDirectory[uint32], t *testing.T) {
				r, err := indexDir.NewReader()
				require.NoError(t, err)

				// read terms
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)

				terms := go_iterators.ToSlice(termsIterator)
				expectedTerms := []string{"term1", "term2"}
				require.EqualValues(t, expectedTerms, terms)

				// read values
				valuesIterator, err := r.ReadValues([]string{"term1", "term2"}, 0, 100)
				require.NoError(t, err)

				values := go_iterators.ToSlice(valuesIterator)
				expectedValues := []uint32{1, 2, 3}
				require.EqualValues(t, expectedValues, values)

				// Close
				require.NoError(t, valuesIterator.Close())
				require.ErrorIs(t, valuesIterator.Close(), go_iterators.ClosedIterator)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirPath, _ := os.MkdirTemp("", "")
			defer os.RemoveAll(dirPath)

			index, err := NewTestIndexDirectory(dirPath)
			require.NoError(t, err)
			tt.prepare(index)
			tt.assert(index, t)
		})
	}
}

func TestItValidatesDirectoryPermissions(t *testing.T) {
	dirPath, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(dirPath)

	// valid directory
	_, err := NewTestIndexDirectory(dirPath)
	require.NoError(t, err)

	// not writable permissions
	require.NoError(t, os.Chmod(dirPath, 0400))
	_, err = NewTestIndexDirectory(dirPath)
	require.ErrorContains(t, err, "the directory is not writable")

	// not writable permissions
	require.NoError(t, os.Chmod(dirPath, 0200))
	_, err = NewTestIndexDirectory(dirPath)
	require.ErrorContains(t, err, "the directory is not readable")
}

func NewTestIndexDirectory(path string) (*IndexDirectory[uint32], error) {
	return NewIndexDirectory[uint32](
		path,
		1000,
		single.CompressUint32,
		single.DecompressUint32,
	)
}
