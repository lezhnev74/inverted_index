package single

import (
	"fmt"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	"path/filepath"
	"testing"
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
			name:    "empty index",
			prepare: func(w InvertedIndexWriter[int]) {},
			assert: func(r InvertedIndexReader[int]) {
				termsIterator, err := r.ReadTerms()
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(termsIterator)
				require.Empty(t, terms)
			},
		},
		{
			name: "duplicate terms",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
				require.ErrorIs(t, w.Put("term", []int{2}), ErrDuplicateTerm)
			},
			assert: func(r InvertedIndexReader[int]) {},
		},
		// ---- VALUES RELATED
		{
			name:    "read values from empty index",
			prepare: func(w InvertedIndexWriter[int]) {},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"term1"}, 0, 100)
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(it)
				require.Empty(t, terms)
			},
		},
		{
			name: "read values for missing terms",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{1}))
			},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"UNKNOWN"}, 0, 100)
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(it)
				require.Empty(t, terms)
			},
		},
		{
			name: "read empty values from index",
			prepare: func(w InvertedIndexWriter[int]) {
				require.NoError(t, w.Put("term", []int{}))
			},
			assert: func(r InvertedIndexReader[int]) {
				it, err := r.ReadValues([]string{"term"}, 0, 100)
				require.NoError(t, err)
				terms := lezhnev74.ToSlice(it)
				require.Empty(t, terms)
			},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, tt.name), func(t *testing.T) {
			dirPath, err := os.MkdirTemp("", "")
			require.NoError(t, err)
			defer os.RemoveAll(dirPath)
			filename := filepath.Join(dirPath, "index")

			indexWriter, err := NewInvertedIndexUnit[int](filename, 10)
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

func TestReadWrite1Term(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 10)
	require.NoError(t, err)
	err = indexWriter.Put("term1", []int{10, 20})
	require.NoError(t, err)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode, read terms only
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)
	termsIterator, err := indexReader.ReadTerms()
	require.NoError(t, err)
	terms := lezhnev74.ToSlice(termsIterator)
	require.EqualValues(t, []string{"term1"}, terms)

	// 2.1 Read again (is it idempotent?)
	termsIterator, err = indexReader.ReadTerms()
	require.NoError(t, err)
	terms = lezhnev74.ToSlice(termsIterator)
	require.EqualValues(t, []string{"term1"}, terms)
}

func TestReadWrite2Terms(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 10)
	require.NoError(t, indexWriter.Put("term1", []int{10, 20}))
	require.NoError(t, indexWriter.Put("term2", []int{1}))
	require.NoError(t, indexWriter.Close())

	// 2. Open the index in a reader-mode, read terms only
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)
	termsIterator, err := indexReader.ReadTerms()
	require.NoError(t, err)
	terms := lezhnev74.ToSlice(termsIterator)
	require.EqualValues(t, []string{"term1", "term2"}, terms)

	// 2.1 Read again (is it idempotent?)
	termsIterator, err = indexReader.ReadTerms()
	require.NoError(t, err)
	terms = lezhnev74.ToSlice(termsIterator)
	require.EqualValues(t, []string{"term1", "term2"}, terms)
}

func TestReadWrite1TermValues(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 10)
	require.NoError(t, err)
	err = indexWriter.Put("term1", []int{10, 20})
	require.NoError(t, err)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)

	// 3. Read values
	valuesIterator, err := indexReader.ReadValues([]string{"term1"}, 0, math.MaxInt)
	require.NoError(t, err)
	timestamps := lezhnev74.ToSlice(valuesIterator)
	require.EqualValues(t, []int{10, 20}, timestamps)
}

func TestReadWrite2TermsValues(t *testing.T) {
	dirPath, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer os.RemoveAll(dirPath)
	filename := filepath.Join(dirPath, "index")

	// 1. Make a new index (open in writer mode), put values and close.
	indexWriter, err := NewInvertedIndexUnit[int](filename, 10)
	require.NoError(t, err)
	err = indexWriter.Put("term1", []int{10, 20})
	require.NoError(t, err)
	err = indexWriter.Put("term2", []int{1, 20, 30})
	require.NoError(t, err)
	err = indexWriter.Close()
	require.NoError(t, err)

	// 2. Open the index in a reader-mode
	indexReader, err := OpenInvertedIndex[int](filename)
	require.NoError(t, err)

	// 3. Read values
	valuesIterator, err := indexReader.ReadValues([]string{"term1", "term2"}, 0, math.MaxInt)
	require.NoError(t, err)
	timestamps := lezhnev74.ToSlice(valuesIterator)
	require.EqualValues(t, []int{1, 10, 20, 30}, timestamps)
}
