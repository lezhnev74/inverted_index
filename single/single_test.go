package single

import (
	"fmt"
	"github.com/blevesearch/vellum"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestAPI(t *testing.T) {
	type test struct {
		name        string
		segmentSize int
		prepare     func(w InvertedIndexWriter[int])
		assert      func(r InvertedIndexReader[int])
	}

	tests := []test{
		// ---- TERMS RELATED
		{
			name:        "empty index",
			segmentSize: 1000,
			prepare:     func(w InvertedIndexWriter[int]) {},
			assert: func(r InvertedIndexReader[int]) {
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(termsIterator)
				require.Empty(t, terms)
			},
		}, {
			name:        "duplicate terms",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
				require.ErrorIs(t, w.Put("term", []int{2}), ErrDuplicateTerm)
			},
			assert: func(r InvertedIndexReader[int]) {},
		}, {
			name:        "idempotent read",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				// 1 pass
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)

				terms := lezhnev74.ToSlice(termsIterator)
				require.EqualValues(t, []string{"term"}, terms)

				// 2 pass
				termsIterator, err = r.ReadTerms()
				require.NoError(t, err)

				terms = lezhnev74.ToSlice(termsIterator)
				require.EqualValues(t, []string{"term"}, terms)
			},
		}, {
			name:        "multiple terms",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1}))
				require.NoError(t, w.Put("term2", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)

				terms := lezhnev74.ToSlice(termsIterator)
				require.EqualValues(t, []string{"term1", "term2"}, terms)
			},
		}, {
			name:        "unordered terms error",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term2", []int{1}))
				require.ErrorIs(t, w.Put("term1", []int{1}), vellum.ErrOutOfOrder)
			},
			assert: func(r InvertedIndexReader[int]) {},
		},
		// ---- VALUES RELATED
		{
			name:        "read values from empty index",
			segmentSize: 1000,
			prepare:     func(w InvertedIndexWriter[int]) {},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"term1"}, 0, 100)
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(it)
				require.Empty(t, terms)
			},
		}, {
			name:        "read values for missing terms",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"UNKNOWN"}, 0, 100)
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(it)
				require.Empty(t, terms)
			},
		}, {
			name:        "read empty values from index",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{}))
			},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"term"}, 0, 100)
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(it)
				require.Empty(t, terms)
			},
		}, {
			name:        "values from 2 terms",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{10, 20}))
				require.NoError(t, w.Put("term2", []int{1, 20, 30}))
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term1", "term2"}, 0, math.MaxInt)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{1, 10, 20, 30}, timestamps)
			},
		}, {
			name:        "read all from multiple segments",
			segmentSize: 2,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 2, 3, 4}))    // 2 segments
				require.NoError(t, w.Put("term2", []int{1, 3, 5, 7, 9})) // 3 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term1", "term2"}, 0, math.MaxInt)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{1, 2, 3, 4, 5, 7, 9}, timestamps)
			},
		}, {
			name:        "scope values",
			segmentSize: 2,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 2, 3, 4})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term1"}, 2, 3)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{2, 3}, timestamps)
			},
		}, {
			name:        "scope values: left boundary is between segments",
			segmentSize: 2,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 5, 10, 20})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term1"}, 9, 999)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{10, 20}, timestamps)
			},
		}, {
			name:        "scope values: right boundary is between segments",
			segmentSize: 2,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 5, 10, 20})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term1"}, 0, 7)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{1, 5}, timestamps)
			},
		}, {
			name:        "scoped values: two terms",
			segmentSize: 2,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 3, 7}))     // 2 segments
				require.NoError(t, w.Put("term2", []int{4, 6, 8, 10})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term1", "term2"}, 7, 999)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{7, 8, 10}, timestamps)
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, tt.name), func(t *testing.T) {
			dirPath, err := os.MkdirTemp("", "")
			require.NoError(t, err)
			defer os.RemoveAll(dirPath)
			filename := filepath.Join(dirPath, "index")

			indexWriter, err := NewInvertedIndexUnit[int](filename, tt.segmentSize)
			require.NoError(t, err)
			tt.prepare(indexWriter)
			err = indexWriter.Close()
			require.NoError(t, err)

			indexReader, err := OpenInvertedIndex[int](filename)
			require.NoError(t, err)
			tt.assert(indexReader)
		})
	}
}

func TestReaderClosesBeforeIteratorIsCompleteFile(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 1)
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)
	it, err := indexReader.ReadValues([]string{"term1"}, 0, 100)
	require.NoError(t, err)

	require.NoError(t, indexReader.Close())
	_, err = it.Next()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestClosedIteratorClosesTheFile(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 1)
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)
	it, err := indexReader.ReadValues([]string{"term1"}, 0, 100)
	require.NoError(t, err)
	require.NoError(t, it.Close())

	_, err = indexReader.(*InvertedIndex[int]).file.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestHugeFile(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 100)
	require.NoError(t, err)

	// generate huge sequences
	values := make([]int, 10_000)
	for i := 0; i < 1_000; i++ {
		for j := 0; j < cap(values); j++ {
			values[j] = i + j
		}
		require.NoError(t, indexWriter.Put(fmt.Sprintf("term%04d", i), values))
	}

	// report file size
	s, _ := indexWriter.(*InvertedIndex[int]).file.Stat()
	fmt.Printf("file size: %d bytes\n", s.Size())

	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)

	// Count total terms in the index
	it, err := indexReader.ReadTerms()
	require.NoError(t, err)
	require.Equal(t, 1_000, len(lezhnev74.ToSlice(it)))

	// Read some values
	it2, err := indexReader.ReadValues([]string{"term1234", "term8521", "term9129"}, 0, 1_000_000)
	require.NoError(t, err)

	lezhnev74.ToSlice(it2)

	require.NoError(t, indexReader.Close())
}
