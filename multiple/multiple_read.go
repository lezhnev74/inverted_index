package multiple

import (
	"fmt"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
	"inverted-index/single"
	"os"
)

func NewMultipleTermsReader(files []string) (lezhnev74.Iterator[string], error) {

	tree := lezhnev74.NewSliceIterator([]string{})

	for _, f := range files {
		r, err := single.OpenInvertedIndex(f, single.DecompressUint32)
		if err != nil {
			tree.Close() // clean up
			return nil, fmt.Errorf("open file %s: %w", f, err)
		}
		it, err := r.ReadTerms()
		if err != nil {
			tree.Close() // clean up
			return nil, fmt.Errorf("read terms %s: %w", f, err)
		}
		tree = lezhnev74.NewUniqueSelectingIterator[string](tree, it, lezhnev74.OrderedCmpFunc[string])
	}

	return tree, nil
}

func NewMultipleValuesReader[T constraints.Ordered](
	files []string,
	unserializeFunc func([]byte) ([]T, error),
	terms []string,
	minValue, maxValue T,
) (lezhnev74.Iterator[T], error) {

	tree := lezhnev74.NewSliceIterator([]T{})

	for _, f := range files {
		r, err := single.OpenInvertedIndex(f, unserializeFunc)
		if err != nil {
			tree.Close() // clean up
			return nil, fmt.Errorf("open file %s: %w", f, err)
		}
		it, err := r.ReadValues(terms, minValue, maxValue)
		tree = lezhnev74.NewUniqueSelectingIterator[T](it, tree, lezhnev74.OrderedCmpFunc[T])
	}

	return tree, nil
}

// SelectFilesForMerging selects a subset of all given files that should be merged together (if any)
func SelectFilesForMerging(files []string, minCount, maxCount int) ([]string, error) {

	if len(files) < minCount {
		return nil, nil
	}

	// sort files by size
	type fsize struct {
		f    string
		size int64
	}
	sizes := make([]fsize, 0, len(files))

	for _, f := range files {
		fstat, err := os.Stat(f)
		if err != nil {
			return nil, fmt.Errorf("file %s size read failed: %w", f, err)
		}
		sizes = append(sizes, fsize{f, fstat.Size()})
	}
	slices.SortFunc(sizes, func(a, b fsize) int {
		if a.size == b.size {
			return 0
		} else if a.size < b.size {
			return -1
		} else {
			return 1
		}
	})

	// pick some
	selectedFiles := make([]string, 0, maxCount)
	for _, fs := range sizes[:min(len(sizes), maxCount)] {
		selectedFiles = append(selectedFiles, fs.f)
	}

	return selectedFiles, nil
}
