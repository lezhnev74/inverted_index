package multiple

import (
	go_iterators "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index/single"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestHugeFile(t *testing.T) {
	indexDir, err := NewIndexDirectory("/home/dmitry/Code/go/src/heaplog2/local/ii", 1000, single.CompressUint64, single.DecompressUint64)
	require.NoError(t, err)

	r, err := indexDir.NewReader()
	require.NoError(t, err)

	_, err = r.ReadTerms()
	require.NoError(t, err)

	err = r.Close()
	require.NoError(t, err)
}

func TestUniqueOnly(t *testing.T) {
	s := []string{"B", "C", "A", "C", "C"}
	require.Equal(t, []string{"A", "B", "C"}, sortUnique(s))
}

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

				err = w.Put("term1", []uint32{1, 2})
				require.NoError(t, err)
				err = w.Put("term2", []uint32{2, 3})
				require.NoError(t, err)
				err = w.Close()
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

				require.NoError(t, r.Close())
				require.NoError(t, r.Close())
			},
		},
		{
			name: "read from multiple files",
			prepare: func(indexDir *IndexDirectory[uint32]) {

				// write to multiple files:
				for i := 0; i < 100; i++ {
					w, err := indexDir.NewWriter()
					require.NoError(t, err)

					err = w.Put("term1", []uint32{1, 2})
					require.NoError(t, err)
					err = w.Put("term2", []uint32{2, 3})
					require.NoError(t, err)

					err = w.Close()
					require.NoError(t, err)
				}

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

			index, err := newTestIndexDirectory(dirPath)
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
	_, err := newTestIndexDirectory(dirPath)
	require.NoError(t, err)

	// not writable permissions
	require.NoError(t, os.Chmod(dirPath, 0400))
	_, err = newTestIndexDirectory(dirPath)
	require.ErrorContains(t, err, "the directory is not writable")

	// not writable permissions
	require.NoError(t, os.Chmod(dirPath, 0200))
	_, err = newTestIndexDirectory(dirPath)
	require.ErrorContains(t, err, "the directory is not readable")
}

func TestItDiscoversFiles(t *testing.T) {
	dirPath, _ := os.MkdirTemp("", "")
	defer os.RemoveAll(dirPath)

	// 1. Populate a directory with files
	dirIndex, err := newTestIndexDirectory(dirPath)
	require.NoError(t, err)

	// 1.1 File A
	w, err := dirIndex.NewWriter()
	require.NoError(t, err)
	w.Put("a", []uint32{1})
	require.NoError(t, w.Close())

	// 1.1 File B
	w, err = dirIndex.NewWriter()
	require.NoError(t, err)
	w.Put("a", []uint32{1})
	require.NoError(t, w.Close())

	// 2. Open a new index from the same dir
	dirIndex2, err := newTestIndexDirectory(dirPath)
	require.NoError(t, err)
	require.Len(t, dirIndex2.currentList.files, 2)
}

//
// func TestMergeExternal(t *testing.T) {
// 	dirPath := "/home/dmitry/Code/go/src/heaplog2/local/ii2/3801863248"
// 	dirIndex, err := NewIndexDirectory[uint64](
// 		dirPath,
// 		1000,
// 		single.CompressUint64,
// 		single.DecompressUint64,
// 	)
// 	require.NoError(t, err)
//
// 	m, err := dirIndex.NewMerger(2, 2)
// 	require.NoError(t, err)
//
// 	files, err := m.Merge()
// 	require.NoError(t, err)
//
// 	log.Printf("%v", files)
// }

func TestCheckMerge(t *testing.T) {

	prepared := []map[string][]uint32{
		{"term0": {0}},
		{"term0": {0}}, // test deduplication
		{"term1": {1}},
		{"term2": {2}},
	}
	cleanup, dirIndex := prepareDirectoryIndex(t, prepared)
	defer cleanup()

	expectedTerms := []string{"term0", "term1", "term2"}
	expectedValues := []uint32{0, 1, 2}

	// this function makes sure merging does not change the data contained in the index
	assertIndex := func() {
		r, err := dirIndex.NewReader()
		require.NoError(t, err)

		// read terms
		termsIterator, err := r.ReadTerms()
		require.NoError(t, err)

		terms := go_iterators.ToSlice(termsIterator)
		require.EqualValues(t, expectedTerms, terms)

		// read values
		valuesIterator, err := r.ReadValues(expectedTerms, 0, 100)
		require.NoError(t, err)

		values := go_iterators.ToSlice(valuesIterator)
		require.EqualValues(t, expectedValues, values)

		// Close
		require.NoError(t, valuesIterator.Close())
		require.ErrorIs(t, valuesIterator.Close(), go_iterators.ClosedIterator)
	}

	merger, err := dirIndex.NewMerger(2, 3) // min/max files to merge
	require.NoError(t, err)

	// Merge files: ITERATION 1
	files, err := merger.Merge()
	require.NoError(t, err)
	require.Len(t, files, 3)
	require.Len(t, dirIndex.currentList.files, 2) // 2 files remain after it is done
	require.Len(t, merger.mergedList.files, 3)    // merged 3 files

	assertIndex()

	// Merge files: ITERATION 2
	files, err = merger.Merge()
	require.NoError(t, err)
	require.Len(t, files, 2)
	require.Len(t, dirIndex.currentList.files, 1) // 1 file remain after it is done
	require.Len(t, merger.mergedList.files, 5)    // merged 2 files + 3 existing

	assertIndex()

	// Merge files: ITERATION 3
	files, err = merger.Merge()
	require.NoError(t, err)
	require.Len(t, files, 0)
	require.Len(t, dirIndex.currentList.files, 1) // 1 file remain after it is done
	require.Len(t, merger.mergedList.files, 5)    // merged 2 files + 3 existing

	assertIndex()

	// Remove merged files
	err = merger.Cleanup()
	require.NoError(t, err)
	require.Len(t, merger.mergedList.files, 0)

	assertIndex()
}

func TestMergedFilesAreKeptUntilReadingIsDone(t *testing.T) {

	prepared := []map[string][]uint32{
		{"term0": {0}},
		{"term1": {1}},
	}
	cleanup, dirIndex := prepareDirectoryIndex(t, prepared)
	defer cleanup()

	// start reading from all files
	reader, err := dirIndex.NewReader()
	require.NoError(t, err)
	iterator, err := reader.ReadTerms()
	require.NoError(t, err)

	// Now merge files
	merger, err := dirIndex.NewMerger(2, 2)
	require.NoError(t, err)

	mergedFiles, err := merger.Merge()
	require.NoError(t, err)
	require.Len(t, mergedFiles, 2)

	// Now attempt to remove merged files
	err = merger.Cleanup()
	require.NoError(t, err)
	require.Len(t, merger.mergedList.files, 2) // See them remain

	// Now close the iterator
	err = iterator.Close()
	require.NoError(t, err)

	// Now attempt to remove merged files again
	err = merger.Cleanup()
	require.NoError(t, err)
	require.Len(t, merger.mergedList.files, 0) // See them gone
}

func TestStressConcurrency(t *testing.T) {
	deadline := time.Now().Add(time.Second) // keep the test this long
	N := 100
	wg := sync.WaitGroup{}
	start := make(chan bool) // use it to start all goroutines at once

	// prepare a directory index
	// Prepare data
	ingestionPayload := []map[string][]uint32{
		{
			"term0": {0, 1, 2, 3},
			"term1": {1, 3, 10},
		},
		{
			"term0": {0, 1, 2, 3},
			"term1": {1, 3, 10},
		},
		{
			"term3": {10, 20},
			"term4": {11, 0},
		},
	}
	cleanup, dirIndex := prepareDirectoryIndex(t, ingestionPayload) // make initial write
	defer cleanup()

	// N goroutines that writes to the index
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start // wait for the start

			for time.Now().Before(deadline) {
				for _, file := range ingestionPayload {
					w, err := dirIndex.NewWriter()
					require.NoError(t, err)

					terms := maps.Keys(file)
					slices.Sort(terms)
					for _, term := range terms {
						err = w.Put(term, file[term])
						require.NoError(t, err)
					}

					err = w.Close()
					require.NoError(t, err)
				}
			}
		}()
	}

	// N goroutines that merges + removes
	merger, err := dirIndex.NewMerger(50, 100)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start // wait for the start

			for time.Now().Before(deadline) {
				_, err := merger.Merge()
				require.NoError(t, err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start // wait for the start

			for time.Now().Before(deadline) {
				err := merger.Cleanup()
				require.NoError(t, err)
			}
		}()
	}

	// N goroutines that reads
	expectedTerms := make([]string, 0)
	for _, payload := range ingestionPayload {
		expectedTerms = append(expectedTerms, maps.Keys(payload)...)
	}
	slices.Sort(expectedTerms)
	expectedTerms = sortUnique(expectedTerms)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-start // wait for the start

			for time.Now().Before(deadline) {
				reader, err := dirIndex.NewReader()
				require.NoError(t, err)

				iterator, err := reader.ReadTerms()
				require.NoError(t, err)

				terms := go_iterators.ToSlice(iterator)
				if len(terms) != len(expectedTerms) {
					log.Printf("err")
				}
				require.EqualValues(t, expectedTerms, terms)

				err = reader.Close()
				require.NoError(t, err)
			}
		}()
	}

	close(start)
	wg.Wait()

	dirIndex.currentList.safeRead(func() {
		log.Printf("total files remaining: %d", len(dirIndex.currentList.files))
	})
}

func prepareDirectoryIndex(t *testing.T, values []map[string][]uint32) (cleanup func(), dirIndex *IndexDirectory[uint32]) {
	dirPath, _ := os.MkdirTemp("", "")
	cleanup = func() { os.RemoveAll(dirPath) }

	var err error
	dirIndex, err = newTestIndexDirectory(dirPath)
	require.NoError(t, err)

	for _, file := range values {
		w, err := dirIndex.NewWriter()
		require.NoError(t, err)

		terms := maps.Keys(file)
		slices.Sort(terms)
		for _, term := range terms {
			err = w.Put(term, file[term])
			require.NoError(t, err)
		}

		err = w.Close()
		require.NoError(t, err)
	}

	return
}

func newTestIndexDirectory(path string) (*IndexDirectory[uint32], error) {
	return NewIndexDirectory[uint32](
		path,
		1000,
		single.CompressUint32,
		single.DecompressUint32,
	)
}
