package single

import (
	"fmt"
	"github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	type test struct {
		name    string
		prepare func(w InvertedIndexWriter[int])
		assert  func(r InvertedIndexReader[int])
	}

	tests := []test{
		// ---- TERMS RELATED
		{
			name: "duplicate terms",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
				require.ErrorIs(t, w.Put("term", []int{2}), ErrDuplicateTerm)
			},
			assert: func(r InvertedIndexReader[int]) {},
		}, {
			name: "idempotent terms read",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				// 1 pass
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)

				terms := go_iterators.ToSlice(termsIterator)
				require.EqualValues(t, []string{"term"}, terms)

				// 2 pass
				termsIterator, err = r.ReadTerms()
				require.NoError(t, err)

				terms = go_iterators.ToSlice(termsIterator)
				require.EqualValues(t, []string{"term"}, terms)
			},
		}, {
			name: "multiple terms",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1}))
				require.NoError(t, w.Put("term2", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)

				terms := go_iterators.ToSlice(termsIterator)
				require.EqualValues(t, []string{"term1", "term2"}, terms)
			},
		},
		// ---- VALUES RELATED
		{
			name: "read values from index with no postings",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{}))
			},
			assert: func(r InvertedIndexReader[int]) {
				values, err := r.ReadValues([]string{"term"}, 0, 100)
				require.NoError(t, err)
				require.Empty(t, values)
			},
		}, {
			name: "no terms read values",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				values, err := r.ReadValues([]string{}, 0, 100)
				require.NoError(t, err)
				require.Empty(t, values)
			},
		}, {
			name: "read values for missing terms",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				values, err := r.ReadValues([]string{"UNKNOWN"}, 0, 100)
				require.NoError(t, err)
				require.Empty(t, values)
			},
		}, {
			name: "read values for partially missing terms",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1}))
				require.NoError(t, w.Put("term2", []int{2}))
			},
			assert: func(r InvertedIndexReader[int]) {
				vals, err := r.ReadValues([]string{"term2", "term3"}, 0, 100)
				require.NoError(t, err)
				require.EqualValues(t, []int{2}, vals)
			},
		}, {
			name: "a single value in a single term",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1"}, 0, 999)
				require.NoError(t, err)
				require.EqualValues(t, []int{1}, timestamps)
			},
		}, {
			name: "read all values",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{10, 20}))
				require.NoError(t, w.Put("term2", []int{1, 20, 30}))
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadAllValues([]string{"term1", "term2"})
				require.NoError(t, err)
				require.EqualValues(t, []int{1, 10, 20, 30}, timestamps)
			},
		}, {
			name: "values from 2 terms",

			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{10, 20}))
				require.NoError(t, w.Put("term2", []int{1, 20, 30}))
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1", "term2"}, 0, math.MaxInt)
				require.NoError(t, err)
				require.EqualValues(t, []int{1, 10, 20, 30}, timestamps)
			},
		}, {
			name: "read all from multiple segments",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 2, 3, 4}))    // 2 segments
				require.NoError(t, w.Put("term2", []int{1, 3, 5, 7, 9})) // 3 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1", "term2"}, 0, math.MaxInt)
				require.NoError(t, err)
				require.EqualValues(t, []int{1, 2, 3, 4, 5, 7, 9}, timestamps)
			},
		}, {
			name: "scope values",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 2, 3, 4})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1"}, 2, 3)
				require.NoError(t, err)
				require.EqualValues(t, []int{2, 3}, timestamps)
			},
		}, {
			name: "scope values: left boundary is between segments",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 5, 10, 20})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1"}, 9, 999)
				require.NoError(t, err)
				require.EqualValues(t, []int{10, 20}, timestamps)
			},
		}, {
			name: "scope values: right boundary is between segments",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 5, 10, 20})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1"}, 0, 7)
				require.NoError(t, err)
				require.EqualValues(t, []int{1, 5}, timestamps)
			},
		}, {
			name: "scoped values: two terms",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1, 3, 7}))      // 2 segments
				require.NoError(t, w.Put("term2", []int{4, 6, 8, 10})) // 2 segments
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term", "term2"}, 7, 999)
				require.NoError(t, err)
				require.EqualValues(t, []int{7, 8, 10}, timestamps)
			},
		}, {
			name: "read values for some terms",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term1", []int{1, 3, 7}))
				require.NoError(t, w.Put("term2", []int{4, 6, 8, 10, 12}))
				require.NoError(t, w.Put("term3", []int{0, 5, 11, 15}))
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term2"}, 0, 999)
				require.NoError(t, err)
				require.EqualValues(t, []int{4, 6, 8, 10, 12}, timestamps)
			},
		}, {
			name: "mixed languages",

			prepare: func(w InvertedIndexWriter[int]) {
				terms := []string{`التقديم`, `חתונה`, `бесплатно`, `zx9uyv`}
				slices.Sort(terms)
				for _, term := range terms {
					require.NoError(t, w.Put(term, []int{1}))
				}
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"бесплатно"}, 0, 999)
				require.NoError(t, err)
				require.EqualValues(t, []int{1}, timestamps)
			},
		}, {
			name: "big chunk",

			prepare: func(w InvertedIndexWriter[int]) {
				terms := []string{"term1"}
				for i := 0; i < 1000; i++ {
					terms = append(terms, fmt.Sprintf("%d", rand.Uint64()))
				}
				slices.Sort(terms)
				for _, term := range terms {
					require.NoError(t, w.Put(term, []int{1}))
				}
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1"}, 0, 999)
				require.NoError(t, err)
				require.EqualValues(t, []int{1}, timestamps)
			},
		}, {
			name: "test big values index",
			prepare: func(w InvertedIndexWriter[int]) {
				terms := []string{"term1"}
				for i := 0; i < 1_00; i++ {
					terms = append(terms, fmt.Sprintf("%d", rand.Uint64()))
				}
				slices.Sort(terms)
				for _, term := range terms {
					values := []int{}
					for i := 0; i < 15_000; i++ {
						values = append(values, i)
					}
					require.NoError(t, w.Put(term, values))
				}
			},
			assert: func(r InvertedIndexReader[int]) {
				timestamps, err := r.ReadValues([]string{"term1"}, 0, 15_000)
				require.NoError(t, err)

				expectedValues := []int{}
				for i := 0; i < 15_000; i++ {
					expectedValues = append(expectedValues, i)
				}

				require.EqualValues(t, expectedValues, timestamps)
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, tt.name), func(t *testing.T) {
			dirPath, err := os.MkdirTemp("", "")
			require.NoError(t, err)
			defer os.RemoveAll(dirPath)
			filename := filepath.Join(dirPath, "index")

			indexWriter, err := NewInvertedIndexUnit[int](filename, CompressGob[int], DecompressGob[int])
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
	indexWriter, err := NewInvertedIndexUnit[int](filename, CompressGob[int], DecompressGob[int])
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// Real size
	fstat, _ := os.Stat(filename)
	require.Equal(t, fstat.Size(), indexWriter.(*InvertedIndex[int]).Len())
}

func TestUseUint32Compression(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[uint32](filename, CompressUint32, DecompressUint32)
	require.NoError(t, err)
	require.NoError(t, indexWriter.Put("term1", []uint32{10, 20})) // <-- two segments will be written (len=1)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[uint32](filename, DecompressUint32)
	require.NoError(t, err)
	values, err := indexReader.ReadValues([]string{"term1"}, 0, 100)
	require.NoError(t, err)
	require.EqualValues(t, []uint32{10, 20}, values)
}

func TestUnableToCreateEmptyIndex(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, CompressGob[int], DecompressGob[int])
	require.NoError(t, err)
	err = indexWriter.Close()
	require.ErrorIs(t, err, ErrEmptyIndex)
}

func TestIntensiveIO(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)

	// 1. Create a new file
	filename := filepath.Join(dirPath, "index")
	indexWriter, err := NewInvertedIndexUnit[uint64](filename, CompressUint64, DecompressUint64)
	require.NoError(t, err)

	const N = 50_000
	allTerms := make([]string, 0, N)
	for i := 0; i < N; i++ {
		term := fmt.Sprintf("%d%d%d", rand.Uint64(), rand.Uint64(), rand.Uint64())
		term = term[:4+rand.Int()%40] + fmt.Sprintf("%d", rand.Uint32())
		allTerms = append(allTerms, term)
	}
	slices.Sort(allTerms)

	for _, term := range allTerms {
		values := []uint64{}
		for j := 0; j < rand.Int()%20; j++ {
			values = append(values, rand.Uint64())
		}
		require.NoError(t, indexWriter.Put(term, values)) // <-- two segments will be written (len=1)
	}

	require.NoError(t, indexWriter.Close())

	//
	// t.Run("write out to disk", func(t *testing.T) {
	// 	tt := time.Now()
	// 	require.NoError(t, indexWriter.Close())
	// 	log.Printf("write out in: %s", time.Now().Sub(tt).String())
	// })

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[uint64](filename, DecompressUint64)
	require.NoError(t, err)

	debug.SetGCPercent(-1)

	t.Run("read from disk", func(t *testing.T) {
		tt := time.Now()
		for _, term := range allTerms {
			_, err := indexReader.ReadAllValues([]string{term})
			require.NoError(t, err)
		}
		log.Printf("read all in: %s", time.Now().Sub(tt).String())
	})
}
