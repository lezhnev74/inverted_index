package single

import (
	"bufio"
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	"github.com/lezhnev74/go-iterators"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/mmap"
	"golang.org/x/exp/slices"
	"io"
	"os"
	"sync"
)

/**

File Layout:

			┌─────*──────┬──8───┬───────4───────┐
			│MinMaxValues│FSTLen│MinMaxValuesLen│
			└────────────┴──────┴───────────────┘
			 \          _______________________/
			  \        /
	┌──*───┬─*─┬──*───┐
	│Values│FST│Footer│
	└──────┴───┴──────┘
  /         \____________
 |                       \
 ┌───*────┬─────┬───*────┐
 │Segment1│ ... │SegmentN│
 └────────┴─────┴────────┘
				/		  \
			   ┌─4──┬──*───┐
			   │Size│Values│
			   └────┴──────┘

*/

var (
	ErrDuplicateTerm = fmt.Errorf("duplicate term inserted")
	ErrEmptyIndex    = fmt.Errorf("no terms inserted")

	writeSize = 4096 * 10
)

// InvertedIndex is a single index piece consists of terms FST + sorted values for each term
// resembles a sorted map[string][]int
// NOT CONCURRENT
type InvertedIndex[V cmp.Ordered] struct {
	file           *os.File
	closeFile      sync.Once
	filePos        uint64
	buf4, buf8     []byte // len buf
	minVal, maxVal V

	// Generic Values --------------------------------------------------
	// serializeSegment is user-provided function to compress values since Value is generic.
	serializeSegment   func(items []V) ([]byte, error)
	unserializeSegment func(data []byte) (items []V, err error)
	cmp                func(a, b V) int // -1,0,1 to impose order on values

	// Writer-mode -----------------------------------------------------
	// Accumulates data and writes our on Close()
	fstBuilder   *vellum.Builder
	fstBuf       *bytes.Buffer
	bufWriter    *bufio.Writer
	bytesWritten uint32
	// termsValues are used to keep ingested data in memory, upon Close() actual writing is happening.
	termsValues []termValue[V] // temporary values for each term

	// Reader-mode -----------------------------------------------------
	// Reads immutable data in the file
	// fst allows to compress terms in the index file
	fst                    *vellum.FST
	fstOffset, indexOffset int64
	mmapFile               *mmap.ReaderAt
}

type termValue[T cmp.Ordered] struct {
	term   string
	values []T
}

type InvertedIndexWriter[V constraints.Ordered] interface {
	io.Closer // flush FST
	// Put accumulates data in memory, writes out on Close.
	Put(term string, values []V) error
}

type InvertedIndexReader[V constraints.Ordered] interface {
	// ReadTerms returns sorted iterator
	ReadTerms() (go_iterators.Iterator[string], error)
	// ReadValues returns sorted values
	ReadValues(terms []string, min V, max V) ([]V, error)
	// ReadAllValues returns sorted values
	ReadAllValues(terms []string) ([]V, error)
	io.Closer
}

func (i *InvertedIndex[V]) Close() error {
	defer i.closeFile.Do(func() { i.file.Close() })

	// writer mode
	if i.fstBuilder != nil {
		if len(i.termsValues) == 0 {
			return ErrEmptyIndex
		}
		return i.write()
	}

	// reader mode
	if i.mmapFile != nil {
		err := i.mmapFile.Close()
		if err != nil {
			return fmt.Errorf("index file close: %w", err)
		}
	}

	return nil
}

func (i *InvertedIndex[V]) Len() int64 { return int64(i.filePos) }

// Put remembers all terms and its values, actual writing is delayed until Close()
func (i *InvertedIndex[V]) Put(term string, values []V) error {

	tv := termValue[V]{term, values}
	j, ok := slices.BinarySearchFunc(i.termsValues, tv, func(a, b termValue[V]) int {
		return cmp.Compare(a.term, b.term)
	})
	if ok {
		return ErrDuplicateTerm
	}

	i.termsValues = slicePutAt(i.termsValues, j, tv)

	return nil
}

func (i *InvertedIndex[V]) ReadValues(terms []string, minVal V, maxVal V) ([]V, error) {

	if len(terms) == 0 {
		return nil, nil
	}

	retValues := make([]V, 0, 100)

	var (
		buf    = make([]byte, 4096)
		n      int
		values []V
	)
	for _, t := range terms {
		valuesOffset, ok, err := i.fst.Get([]byte(t))
		if err != nil {
			return nil, fmt.Errorf("read values: %w", err)
		}
		if !ok {
			continue
		}

		// segment length is contained in the first 4 bytes:
		n, err = i.mmapFile.ReadAt(buf, int64(valuesOffset))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("read values: %w", err)
		}

		segmentSize := binary.BigEndian.Uint32(buf[:4])
		if segmentSize > uint32(n-4) {
			// we need to read more
			buf = make([]byte, segmentSize+4)

			n, err = i.mmapFile.ReadAt(buf, int64(valuesOffset))
			if err != nil {
				return nil, fmt.Errorf("read values: %w", err)
			}
		}

		values, err = i.unserializeSegment(buf[4 : segmentSize+4])
		if err != nil {
			return nil, fmt.Errorf("read values: unserialize: %w", err)
		}

		retValues = append(retValues, values...)
	}

	retValues = sliceSortUnique(retValues)
	retValues = sliceFilterInPlace(retValues, func(v V) bool {
		return v >= minVal && v <= maxVal
	})

	return retValues, nil
}

func (i *InvertedIndex[V]) ReadAllValues(terms []string) ([]V, error) {
	return i.ReadValues(terms, i.minVal, i.maxVal)
}

func (i *InvertedIndex[V]) ReadTerms() (go_iterators.Iterator[string], error) {
	it, err := i.fst.Iterator(nil, nil)
	if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
		return nil, err
	} else if errors.Is(err, vellum.ErrIteratorDone) {
		return go_iterators.NewSliceIterator([]string{}), nil
	}

	var lastErr error
	itWrap := go_iterators.NewCallbackIterator(
		func() (v string, err error) {
			if lastErr != nil {
				if errors.Is(lastErr, vellum.ErrIteratorDone) {
					lastErr = go_iterators.EmptyIterator // override the internal error
				}
				return "", lastErr
			}
			term, _ := it.Current()
			if len(term) == 0 {
				return "", go_iterators.ClosedIterator
			}
			v = string(term)
			lastErr = it.Next()
			return
		},
		func() error {
			return nil
		})

	return itWrap, nil
}

// slicePutAt is an efficient insertion function that avoid unnecessary allocations
func slicePutAt[V any](dst []V, pos int, v V) []V {
	dst = append(dst, v) // could grow here
	copy(dst[pos+1:], dst[pos:])
	dst[pos] = v
	return dst
}

// sliceFilterInPlace does not allocate
func sliceFilterInPlace[T any](input []T, filter func(elem T) bool) []T {
	n := 0
	for _, elem := range input {
		if filter(elem) {
			input[n] = elem
			n++
		}
	}
	return input[:n]
}

// sliceSortUnique removes duplicates in place, returns sorted values
func sliceSortUnique[V constraints.Ordered](s []V) []V {
	slices.Sort(s)

	if len(s) == 0 {
		return s
	}

	k := 1
	for i := 1; i < len(s); i++ {
		if s[i] != s[k-1] {
			s[k] = s[i]
			k++
		}
	}

	return s[:k]
}

func (i *InvertedIndex[V]) writeFooter(fstL int) error {

	values := []V{i.minVal, i.maxVal}
	valuesBuf, err := i.serializeSegment(values)
	if err != nil {
		return fmt.Errorf("footer: failed compressing min/max values: %w", err)
	}

	n, err := i.bufWriter.Write(valuesBuf)
	if err != nil {
		return fmt.Errorf("footer: failed writing min/max values: %w", err)
	}
	i.filePos += uint64(n)

	binary.BigEndian.PutUint64(i.buf8, uint64(fstL))
	n, err = i.bufWriter.Write(i.buf8)
	if err != nil {
		return fmt.Errorf("fst: failed writing size: %w", err)
	}
	i.filePos += uint64(n)

	binary.BigEndian.PutUint32(i.buf4, uint32(len(valuesBuf)))
	n, err = i.bufWriter.Write(i.buf4)
	if err != nil {
		return fmt.Errorf("footer: failed writing min/max values size: %w", err)
	}
	i.filePos += uint64(n)

	return nil
}

// write flushes all in-memory data to the file
func (i *InvertedIndex[V]) write() error {

	var (
		err              error
		serializedValues []byte
		n                int
		mmSet            bool
	)

	i.bufWriter = bufio.NewWriterSize(i.file, writeSize)

	// 1. sort accumulated terms
	slices.SortFunc(i.termsValues, func(a, b termValue[V]) int {
		return cmp.Compare(a.term, b.term)
	})

	// 2. write out:
	for _, tv := range i.termsValues {
		// 	2.1 write to FST current file pos
		err = i.fstBuilder.Insert([]byte(tv.term), i.filePos)
		if err != nil {
			return fmt.Errorf("fst: insert: %w", err)
		}
		//  2.2 write compressed term values
		serializedValues, err = i.serializeSegment(tv.values)
		binary.BigEndian.PutUint32(i.buf4, uint32(len(serializedValues)))
		n, err = i.bufWriter.Write(i.buf4)
		if err != nil {
			return fmt.Errorf("write segment size: %w", err)
		}
		i.filePos += uint64(n)
		n, err = i.bufWriter.Write(serializedValues)
		if err != nil {
			return fmt.Errorf("write segment: %w", err)
		}
		i.filePos += uint64(n)
		//  2.3 remember min-max values
		for _, v := range tv.values {
			if !mmSet {
				i.minVal = v
				i.maxVal = v
				mmSet = true
			} else {
				i.minVal = min(i.minVal, v)
				i.maxVal = max(i.maxVal, v)
			}
		}
	}

	// 3. Write fst + footer
	err = i.fstBuilder.Close()
	if err != nil {
		return fmt.Errorf("fst: close: %w", err)
	}
	i.fstBuilder = nil
	n, err = i.bufWriter.Write(i.fstBuf.Bytes())
	if err != nil {
		return fmt.Errorf("fst: failed writing: %w", err)
	}
	i.fstBuf = nil // free up
	i.filePos += uint64(n)

	// write the footer
	err = i.writeFooter(n)
	if err != nil {
		return err
	}

	err = i.bufWriter.Flush()
	if err != nil {
		return fmt.Errorf("failed writing: %w", err)
	}
	i.bufWriter = nil

	return nil
}

func (i *InvertedIndex[V]) readFooter() (
	fst *vellum.FST,
	fstOffset int64,
	minValue V,
	maxValue V,
	err error,
) {

	buf := make([]byte, 4096)
	fstLenSize := int64(8)
	minMaxSize := int64(4)
	tailSize := fstLenSize + minMaxSize

	var (
		fstLen int64
		mmBuf  []byte
	)

	// read the end of the file for parsing footer
	fileSize := int64(i.mmapFile.Len())

	if fileSize <= tailSize {
		// This is a naive protection, works for empty files, does not cover all corruptions.
		// For proper file consistency it should check a hashsum.
		err = fmt.Errorf("the file size is too small (%d bytes), probably corrupted", fileSize)
		return
	}

	p := min(fileSize, int64(len(buf)))
	offset := fileSize - p

	n, err := i.mmapFile.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("read footer: length: %w", err)
		return
	}

	// extract footer numbers
	minMaxLen := int64(binary.BigEndian.Uint32(buf[int64(n)-minMaxSize : int64(n)]))

	fstLen = int64(binary.BigEndian.Uint64(buf[int64(n)-minMaxSize-fstLenSize : int64(n)-minMaxSize]))

	fstOffset = fileSize - minMaxLen - fstLen - minMaxSize - fstLenSize

	// read fst body (fst goes before minmax values, so when we have that, we have everything)
	if fstLen > int64(n)-minMaxLen-tailSize { // do we need to read more bytes?
		// yes, we need to read more
		p = fstLen + minMaxLen + tailSize
		offset = fileSize - p

		// read both fst + mmvalues
		buf = make([]byte, fstLen+minMaxLen)
		n, err = i.mmapFile.ReadAt(buf, offset)
		if err != nil {
			return
		}

		mmBuf = buf[n-int(minMaxLen):]
		buf = buf[:n-int(minMaxLen)]

	} else {
		// if FST is in the buf already, that means minMax values are there too
		mmBuf = buf[n-int(minMaxLen+tailSize) : int64(n)-tailSize]
		buf = buf[n-int(fstLen+minMaxLen+tailSize) : int64(n)-tailSize-minMaxLen]
	}

	fst, err = vellum.Load(buf)
	if err != nil {
		err = fmt.Errorf("fst: load failed: %w", err)
		return
	}

	values, err := i.unserializeSegment(mmBuf)
	if err != nil {
		return
	}
	minValue, maxValue = values[0], values[1]

	return
}

func NewInvertedIndexUnit[V constraints.Ordered](
	filename string,
	serializeValues func([]V) ([]byte, error),
	unserializeValues func([]byte) ([]V, error),
) (InvertedIndexWriter[V], error) {
	var err error
	iiw := &InvertedIndex[V]{
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		buf8:               make([]byte, 8),
		serializeSegment:   serializeValues,
		unserializeSegment: unserializeValues,
	}

	iiw.file, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	iiw.fstBuilder, err = vellum.New(iiw.fstBuf, nil)

	return iiw, err
}

func OpenInvertedIndex[V constraints.Ordered](
	file string,
	unserializeValues func([]byte) ([]V, error),
) (InvertedIndexReader[V], error) {
	f, err := mmap.Open(file)
	if err != nil {
		return nil, err
	}

	i := &InvertedIndex[V]{
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		unserializeSegment: unserializeValues,
		cmp:                cmp.Compare[V],
		mmapFile:           f,
	}

	fst, fstOffset, minValue, maxValue, err := i.readFooter()
	if err != nil {
		return nil, err
	}

	i.fst = fst
	i.fstOffset = fstOffset
	i.minVal = minValue
	i.maxVal = maxValue

	return i, nil
}
