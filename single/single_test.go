package single

import (
	"fmt"
	"github.com/blevesearch/vellum"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	type test struct {
		name        string
		segmentSize uint32
		prepare     func(w InvertedIndexWriter[int])
		assert      func(r InvertedIndexReader[int])
	}

	tests := []test{
		// ---- TERMS RELATED
		{
			name:        "duplicate terms",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
				require.ErrorIs(t, w.Put("term", []int{2}), ErrDuplicateTerm)
			},
			assert: func(r InvertedIndexReader[int]) {},
		}, {
			name:        "idempotent terms read",
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
			name:        "read values from index with no postings",
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
			name:        "read values for partially missing terms",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1}))
				require.NoError(t, w.Put("term2", []int{2}))
			},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"term2", "term3"}, 0, 100)
				require.NoError(t, err)
				vals := lezhnev74.ToSlice(it)
				require.EqualValues(t, []int{2}, vals)
			},
		}, {
			name:        "read all values",
			segmentSize: 1000,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{10, 20}))
				require.NoError(t, w.Put("term2", []int{1, 20, 30}))
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadAllValues([]string{"term1", "term2"})
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{1, 10, 20, 30}, timestamps)
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
				require.NoError(t, w.Put("term", []int{1, 3, 7}))      // 2 segments
				require.NoError(t, w.Put("term2", []int{4, 6, 8, 10})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term", "term2"}, 7, 999)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{7, 8, 10}, timestamps)
			},
		}, {
			name:        "read values for some terms",
			segmentSize: 2,
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 3, 7}))
				require.NoError(t, w.Put("term2", []int{4, 6, 8, 10, 12}))
				require.NoError(t, w.Put("term3", []int{0, 5, 11, 15}))
			},
			assert: func(r InvertedIndexReader[int]) {
				valuesIterator, err := r.ReadValues([]string{"term2"}, 0, 999)
				require.NoError(t, err)
				timestamps := lezhnev74.ToSlice(valuesIterator)
				require.EqualValues(t, []int{4, 6, 8, 10, 12}, timestamps)
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, tt.name), func(t *testing.T) {
			dirPath, err := os.MkdirTemp("", "")
			require.NoError(t, err)
			defer os.RemoveAll(dirPath)
			filename := filepath.Join(dirPath, "index")

			indexWriter, err := NewInvertedIndexUnit[int](filename, tt.segmentSize, CompressGob[int], DecompressGob[int])
			require.NoError(t, err)
			tt.prepare(indexWriter)
			err = indexWriter.Close()
			require.NoError(t, err)

			indexReader, err := OpenInvertedIndex[int](filename, DecompressGob[int])
			require.NoError(t, err)
			tt.assert(indexReader)
		})
	}
}

func TestFileSizeReported(t *testing.T) {
	// after the index is closed it remembers the written size, so we can avoid further fstat calls.

	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// Make a new index
	indexWriter, err := NewInvertedIndexUnit[int](filename, 1, CompressGob[int], DecompressGob[int])
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// Real size
	fstat, _ := os.Stat(filename)
	require.Equal(t, fstat.Size(), indexWriter.Len())
}

func TestUseUint32Compression(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[uint32](filename, 1, CompressUint32, DecompressUint32)
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []uint32{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[uint32](filename, DecompressUint32)
	require.NoError(t, err)
	it, err := indexReader.ReadValues([]string{"term1"}, 0, 100)
	require.NoError(t, err)
	values := lezhnev74.ToSlice(it)
	require.EqualValues(t, []uint32{10, 20}, values)
}

func TestReaderClosesBeforeIteratorIsCompleteFile(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 1, CompressGob[int], DecompressGob[int])
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename, DecompressGob[int])
	require.NoError(t, err)
	it, err := indexReader.ReadValues([]string{"term1"}, 0, 100)
	require.NoError(t, err)

	require.NoError(t, indexReader.Close())
	_, err = it.Next()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestUnableToCreateEmptyIndex(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 1, CompressGob[int], DecompressGob[int])
	require.NoError(t, err)
	err = indexWriter.Close()
	require.ErrorIs(t, err, ErrEmptyIndex)
}

func TestClosedIteratorClosesTheFile(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 1, CompressGob[int], DecompressGob[int])
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename, DecompressGob[int])
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
	indexWriter, err := NewInvertedIndexUnit[uint32](filename, 997, CompressUint32, DecompressUint32)
	require.NoError(t, err)

	// generate huge sequences
	rand.Seed(uint64(time.Now().UnixNano()))
	terms := make([]string, 100)
	sample := make([]string, 0)
	for i := 0; i < len(terms); i++ {
		terms[i] = fmt.Sprintf("%030d", rand.Uint64())
		if rand.Int()%1 == 0 {
			sample = append(sample, terms[i])
		}
	}
	slices.Sort(terms)

	for i := 0; i < len(terms); i++ {
		values := make([]uint32, 1000)
		for j := 0; j < len(values); j++ {
			values[j] = rand.Uint32()
		}
		require.NoError(t, indexWriter.Put(terms[i], values))
	}

	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex(filename, DecompressUint32)
	require.NoError(t, err)

	// Count total terms in the index
	it, err := indexReader.ReadTerms()
	require.NoError(t, err)
	allTerms := lezhnev74.ToSlice(it)
	require.Equal(t, 100, len(allTerms))

	// Read sampled terms
	for _, ts := range sample {
		require.Contains(t, allTerms, ts)
	}

	// Read some values
	it2, err := indexReader.ReadValues(sample[:1], 0, math.MaxUint32)
	require.NoError(t, err)

	v := lezhnev74.ToSlice(it2)
	require.Equal(t, 1000, len(v))

	require.NoError(t, indexReader.Close())

	// show summary
	PrintSummary(filename, os.Stdout)
}
