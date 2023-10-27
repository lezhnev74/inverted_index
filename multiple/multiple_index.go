package multiple

import (
	"cmp"
	"fmt"
	"github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index/single"
	"golang.org/x/exp/constraints"
	"os"
	"path"
	"time"
)

// IndexDirectory manages multiple index files in a directory.
// it supports the same read/write API as a single index file
// and also manages concurrent merging process (removing merged files to keep the disk space low).
type IndexDirectory[T constraints.Ordered] struct {
	directoryPath           string
	currentList, mergedList *filesList

	// index config
	segmentSize       uint32
	serializeValues   func([]T) ([]byte, error)
	unserializeValues func([]byte) ([]T, error)
}

type IndexDirectoryWriter[T constraints.Ordered] struct {
	indexDirectory *IndexDirectory[T]
	internalWriter single.InvertedIndexWriter[T]
	filename       string
}

func (d *IndexDirectory[T]) NewWriter() (single.InvertedIndexWriter[T], error) {
	w := &IndexDirectoryWriter[T]{
		indexDirectory: d,
		filename:       path.Join(d.directoryPath, fmt.Sprintf("%d", time.Now().UnixMicro())), // filename selection
	}

	var err error

	w.internalWriter, err = single.NewInvertedIndexUnit[T](w.filename, d.segmentSize, d.serializeValues, d.unserializeValues)
	if err != nil {
		defer os.Remove(w.filename)
		return nil, fmt.Errorf("unable to make a new index: %w", err)
	}

	return w, nil
}

func (i *IndexDirectoryWriter[T]) Close() error {
	if err := i.internalWriter.Close(); err != nil {
		return err
	}

	// add the file to the active list
	i.indexDirectory.currentList.safeWrite(func() {
		i.indexDirectory.currentList.putFile(
			i.filename,
			i.internalWriter.(*single.InvertedIndex[T]).Len(),
		)
	})

	return nil
}

func (i *IndexDirectoryWriter[T]) Put(term string, values []T) error {
	return i.internalWriter.Put(term, values)
}

type IndexDirectoryReader[T constraints.Ordered] struct {
	indexDirectory *IndexDirectory[T]
}

// NewReader makes a new InvertedIndexReader that hides multiple files.
// A reader multiplex readers from individual files and allows early closing via Close() call.
func (d *IndexDirectory[T]) NewReader() (*IndexDirectoryReader[T], error) {
	r := &IndexDirectoryReader[T]{
		indexDirectory: d,
	}
	return r, nil
}

func (i *IndexDirectoryReader[T]) ReadTerms() (go_iterators.Iterator[string], error) {

	iteratorSelect := func(ii single.InvertedIndexReader[T]) (go_iterators.Iterator[string], error) {
		return ii.ReadTerms()
	}

	return joinIterators(
		i,
		go_iterators.NewSliceIterator([]string{}),
		iteratorSelect,
	)
}

// joinIterators creates a selection tree from all index files' iterators.
// it makes sure cleanup is happening when the reading is over.
func joinIterators[T constraints.Ordered, V constraints.Ordered](
	i *IndexDirectoryReader[T],
	iterator go_iterators.Iterator[V],
	selectIterator func(ii single.InvertedIndexReader[T]) (go_iterators.Iterator[V], error),
) (go_iterators.Iterator[V], error) {
	// acquiring index read lock is enough here
	// as no logic is allowed to remove a file without a write lock acquired
	fileList := i.indexDirectory.currentList
	fileList.lock.RLock()
	defer fileList.lock.RUnlock()

	openIndexes := make(map[*indexFile]single.InvertedIndexReader[T], len(fileList.files))

	for _, indexFile := range fileList.files {

		ii, err := single.OpenInvertedIndex(indexFile.path, i.indexDirectory.unserializeValues)
		if err != nil {
			return nil, fmt.Errorf("open index at %s: %w", indexFile.path, err)
		}
		openIndexes[indexFile] = ii

		selectedIterator, err := selectIterator(ii)
		if err != nil {
			_ = ii.Close()
			return nil, fmt.Errorf("read segments from %s: %w", indexFile.path, err)
		}

		indexFile.rlock.RLock()

		// wrap up the iterator to clean up upon closing
		selectedIterator = go_iterators.NewClosingIterator(selectedIterator, func(innerErr error) (err error) {
			defer indexFile.rlock.RUnlock() // release the underlying file
			defer func() {
				iteratorCloseError := ii.Close()
				if err != nil {
					err = iteratorCloseError // report the iterator close error
				}
			}()

			if innerErr != nil {
				return innerErr
			}

			return
		})

		iterator = go_iterators.NewUniqueSelectingIterator(iterator, selectedIterator, cmp.Compare[V])
	}

	return iterator, nil
}

func (i *IndexDirectoryReader[T]) ReadValues(terms []string, min T, max T) (go_iterators.Iterator[T], error) {
	iteratorSelect := func(ii single.InvertedIndexReader[T]) (go_iterators.Iterator[T], error) {
		return ii.ReadValues(terms, min, max)
	}

	return joinIterators(
		i,
		go_iterators.NewSliceIterator([]T{}),
		iteratorSelect,
	)
}

func (i *IndexDirectoryReader[T]) Close() error {

	panic("implement me")
}

func NewIndexDirectory[T constraints.Ordered](
	path string,
	segmentSize uint32,
	serializeValues func([]T) ([]byte, error),
	unserializeValues func([]byte) ([]T, error),
) (*IndexDirectory[T], error) {
	if err := checkPermissions(path); err != nil {
		return nil, err
	}

	index := &IndexDirectory[T]{
		directoryPath: path,
		currentList:   NewFilesList(),
		mergedList:    NewFilesList(),

		segmentSize:       segmentSize,
		serializeValues:   serializeValues,
		unserializeValues: unserializeValues,
	}

	return index, nil
}

// checkPermissions tests the directory for read/write permissions.
func checkPermissions(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("bad index directory: %w", err)
	}

	if !s.IsDir() {
		return fmt.Errorf("the given path is not a directory")
	}

	mode := s.Mode().Perm()

	if mode&0200 == 0 && mode&0020 == 0 && mode&0002 == 0 {
		return fmt.Errorf("the directory is not writable: %s", path)
	}

	if mode&0400 == 0 && mode&0040 == 0 && mode&0004 == 0 {
		return fmt.Errorf("the directory is not readable: %s", path)
	}

	return nil
}
