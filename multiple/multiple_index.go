package multiple

import (
	"cmp"
	"fmt"
	"github.com/lezhnev74/go-iterators"
	"github.com/lezhnev74/inverted_index/single"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
	"log"
	"math/rand"
	"os"
	"path"
	"time"
)

// IndexDirectory manages multiple index files in a directory.
// it supports the same read/write API as a single index file
// and also manages concurrent merging process (removing merged files to keep the disk space low).
type IndexDirectory[T constraints.Ordered] struct {
	directoryPath string
	currentList   *filesList

	// index config
	segmentSize       uint32
	serializeValues   func([]T) ([]byte, error)
	unserializeValues func([]byte) ([]T, error)
}

type DirectoryIndexMerger[T constraints.Ordered] struct {
	indexDirectory    *IndexDirectory[T]
	minFile, maxFiles int // how much to merge at one run
	merging           map[string]struct{}
	mergedList        *filesList
}

// Merge is synchronous.
// It selects files for merging and proceeds the merge operation.
// It returns merged files.
func (m *DirectoryIndexMerger[T]) Merge() (files []*indexFile, err error) {
	n := time.Now()

	files, err = m.CheckMerge()
	if err != nil {
		return nil, fmt.Errorf("merging failed to start: %w", err)
	}

	if len(files) == 0 {
		return // nothing to merge
	}

	totalSize := int64(0)
	mergePaths := make([]string, 0, len(files))
	for _, f := range files {
		totalSize += f.len
		mergePaths = append(mergePaths, f.path)
	}

	mergedFilename := m.indexDirectory.selectFilename()

	mergedFileLen, err := m.mergeFiles(mergedFilename, mergePaths)
	if err != nil {
		return nil, fmt.Errorf("merging failed: %w", err)
	}

	m.indexDirectory.currentList.putFile(mergedFilename, mergedFileLen)

	// move the merged files to another files list for removal
	m.indexDirectory.currentList.removeFiles(files)
	for _, f := range files {
		m.mergedList.putFileP(f)
	}

	log.Printf(
		"merged %d files (%d bytes total) in %s, new file size is %d bytes\n",
		len(files),
		totalSize,
		time.Now().Sub(n).String(),
		mergedFileLen,
	)

	return files, nil
}

// CheckMerge marks and returns files for merging.
func (m *DirectoryIndexMerger[T]) CheckMerge() ([]*indexFile, error) {

	// do not merge less than minMerge files
	mergeBatch := make([]*indexFile, 0, m.maxFiles)

	fileList := m.indexDirectory.currentList
	fileList.safeRead(func() {
		// files are sorted by len
		for i := 0; i < len(fileList.files); i++ {

			if _, ok := m.merging[fileList.files[i].path]; ok {
				continue
			}

			// at this point I want to mark the file as being merged,
			// but this file can be read at the moment, so I can't modify it
			m.merging[fileList.files[i].path] = struct{}{}

			mergeBatch = append(mergeBatch, fileList.files[i])

			if len(mergeBatch) == cap(mergeBatch) {
				break
			}
		}
	})

	if len(mergeBatch) < m.minFile {
		return nil, nil
	}

	return mergeBatch, nil
}

func (m *DirectoryIndexMerger[T]) mergeFiles(dstFile string, srcFiles []string) (int64, error) {
	// copy all terms to memory and release file pointers.
	allTerms := make([]string, 0)

	// below code uses one file descriptor at a time:
	for _, f := range srcFiles {
		r, err := single.OpenInvertedIndex(f, m.indexDirectory.unserializeValues)
		if err != nil {
			return 0, fmt.Errorf("open file %s: %w", f, err)
		}

		it, err := r.ReadTerms()
		if err != nil {
			return 0, fmt.Errorf("read terms %s: %w", f, err)
		}

		allTerms = append(allTerms, go_iterators.ToSlice(it)...)

		err = r.Close()
		if err != nil {
			return 0, fmt.Errorf("read terms %s: %w", f, err)
		}
	}

	// sort all the terms
	slices.Sort(allTerms)

	// prepare the new index file:
	dstIndex, err := single.NewInvertedIndexUnit(dstFile, m.indexDirectory.segmentSize, m.indexDirectory.serializeValues, m.indexDirectory.unserializeValues)
	if err != nil {
		return 0, fmt.Errorf("unable to create new index %s: %w", dstFile, err)
	}

	// for each term, read all the values and push to the new index:
	for _, term := range allTerms {
		// get all values for this term from all indexes
		termValues := make([]T, 0)
		for _, srcFile := range srcFiles {
			r, err := single.OpenInvertedIndex(srcFile, m.indexDirectory.unserializeValues)
			if err != nil {
				return 0, fmt.Errorf("open file %s: %w", srcFile, err)
			}

			vIt, err := r.ReadAllValues([]string{term})
			if err != nil {
				return 0, fmt.Errorf("read term values: %w", err)
			}

			termValues = append(termValues, go_iterators.ToSlice(vIt)...)

			err = r.Close()
			if err != nil {
				return 0, fmt.Errorf("read term values: %w", err)
			}
		}

		err = dstIndex.Put(term, termValues)
		if err != nil {
			return 0, fmt.Errorf("put to new index: %w", err)
		}
	}

	err = dstIndex.Close()
	size := dstIndex.(*single.InvertedIndex[T]).Len()
	return size, err
}

func (m *DirectoryIndexMerger[T]) Cleanup() (err error) {
	m.mergedList.safeWrite(func() {
		removedFiles := make([]*indexFile, 0)

		for _, file := range m.mergedList.files {
			if !file.lock.TryLock() {
				continue
			}
			file.lock.Unlock()

			removeErr := os.Remove(file.path)
			if removeErr != nil {
				err = removeErr
			} else {
				removedFiles = append(removedFiles, file)
			}
		}

		// forget removed files
		x := 0
		for i := 0; i < len(m.mergedList.files); i++ {
			f := m.mergedList.files[i]
			if !slices.Contains(removedFiles, f) {
				m.mergedList.files[x] = f
				x++
			}
		}
		m.mergedList.files = m.mergedList.files[:x]
	})

	return
}

type IndexDirectoryWriter[T constraints.Ordered] struct {
	indexDirectory *IndexDirectory[T]
	internalWriter single.InvertedIndexWriter[T]
	filename       string
}

func (d *IndexDirectory[T]) NewWriter() (single.InvertedIndexWriter[T], error) {

	filename := d.selectFilename()

	w := &IndexDirectoryWriter[T]{
		indexDirectory: d,
		filename:       filename,
	}

	var err error

	w.internalWriter, err = single.NewInvertedIndexUnit[T](w.filename, d.segmentSize, d.serializeValues, d.unserializeValues)
	if err != nil {
		defer os.Remove(w.filename)
		return nil, fmt.Errorf("unable to make a new index: %w", err)
	}

	return w, nil
}

func (d *IndexDirectory[T]) selectFilename() string {
	// filename selection
	filename := path.Join(
		d.directoryPath,
		fmt.Sprintf("%d_%d", time.Now().UnixMicro(), rand.Int()),
	)
	return filename
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

func (d *IndexDirectory[T]) NewMerger(min, max int) (*DirectoryIndexMerger[T], error) {
	m := &DirectoryIndexMerger[T]{
		indexDirectory: d,
		minFile:        min,
		maxFiles:       max,
		merging:        make(map[string]struct{}),
		mergedList:     NewFilesList(),
	}
	return m, nil
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

	for _, singleFile := range fileList.files {
		singleFile := singleFile // capture the variable

		ii, err := single.OpenInvertedIndex(singleFile.path, i.indexDirectory.unserializeValues)
		if err != nil {
			return nil, fmt.Errorf("open index at %s: %w", singleFile.path, err)
		}

		selectedIterator, err := selectIterator(ii)
		if err != nil {
			_ = ii.Close()
			return nil, fmt.Errorf("read segments from %s: %w", singleFile.path, err)
		}

		singleFile.lock.RLock()

		// wrap up the iterator to clean up upon closing
		selectedIterator = go_iterators.NewClosingIterator(selectedIterator, func(innerErr error) (err error) {
			defer singleFile.lock.RUnlock() // release the underlying file
			defer func() {
				iteratorCloseError := ii.Close()
				if err != nil {
					err = iteratorCloseError // report the iterator close error
				}
			}()

			return innerErr
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
	// no closing is happening here as actual closing must be called on the iterators received from the reader apps.
	return nil
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
