package multiple

import (
	"errors"
	"fmt"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index/single"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"

	"os"
)

func MergeIndexes[V constraints.Ordered](
	src []string,
	dst string,
	segmentSize uint32,
	serializeValues func([]V) ([]byte, error),
	unserializeValues func([]byte) ([]V, error),
) error {

	var term string
	allTermsIterator := lezhnev74.NewSliceIterator([]string{})
	defer allTermsIterator.Close()
	srcIndexes := make([]single.InvertedIndexReader[V], 0, len(src))

	for _, f := range src {
		r, err := single.OpenInvertedIndex(f, unserializeValues)
		if err != nil {
			return fmt.Errorf("open file %s: %w", f, err)
		}
		srcIndexes = append(srcIndexes, r)

		it, err := r.ReadTerms()
		if err != nil {
			return fmt.Errorf("read terms %s: %w", f, err)
		}

		allTermsIterator = lezhnev74.NewUniqueSelectingIterator[string](allTermsIterator, it, lezhnev74.OrderedCmpFunc[string])
	}

	w, err := single.NewInvertedIndexUnit(dst, segmentSize, serializeValues, unserializeValues)
	if err != nil {
		return fmt.Errorf("create new index %s: %w", dst, err)
	}

	for {
		term, err = allTermsIterator.Next()
		if errors.Is(err, lezhnev74.EmptyIterator) {
			break
		} else if err != nil {
			return fmt.Errorf("read merged terms: %w", err)
		}

		// get all values for this term from all indexes
		tvIt := lezhnev74.NewSliceIterator([]V{})
		for _, r := range srcIndexes {
			vIt, err := r.ReadAllValues([]string{term})
			if err != nil {
				return fmt.Errorf("read term values: %w", err)
			}
			tvIt = lezhnev74.NewUniqueSelectingIterator(tvIt, vIt, lezhnev74.OrderedCmpFunc[V])
		}
		allTermValues := lezhnev74.ToSlice(tvIt)

		err = w.Put(term, allTermValues)
		if err != nil {
			return fmt.Errorf("put to new index: %w", err)
		}
	}

	return w.Close()
}

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
