package multiple

import (
	"fmt"
	lezhnev74 "github.com/lezhnev74/go-iterators"
	"golang.org/x/exp/constraints"
	"inverted-index/single"
)

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
