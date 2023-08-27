package single

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/vellum"
	"github.com/lezhnev74/go-iterators"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
	"io"
	"os"
	"sync"
)

/**

			File Layout:

           ┌──────────────┬─────┬──────────────┬─────┬────────┐
           │ term1 values │ ... │ termN values │ FST │ FSTLEN │
           └──────────────┴─────┴──────────────┴─────┴────────┘
      ____/                \___________________
     |                                         \
     ┌────────┬─────┬────────┬─────────┬────────┐
     │segment1│ ... │segmentN│indexBody│IndexLen│
     └────────┴─────┴────────┴─────────┴────────┘

*/

var (
	ErrDuplicateTerm = fmt.Errorf("duplicate term inserted")
)

// InvertedIndex is a single index piece consists of terms FST + sorted values for each term
// resembles a sorted map[string][]int
// NOT CONCURRENT
type InvertedIndex[V cmp.Ordered] struct {
	file       *os.File
	closeFile  sync.Once
	filePos    int64
	buf4, buf8 []byte // len buf

	// Values
	serializeSegment   func(items []V) ([]byte, error)
	unserializeSegment func(data []byte) (items []V, err error)
	cmp                func(a, b V) int // -1,0,1 to impose order on values

	// write mode
	fstBuilder        *vellum.Builder
	bytesWritten      uint32
	segmentSize       uint32 // in number of values in a segment
	lastTerm          string
	termsValues       [][]V // temporary values for each term
	valuesIndexOffset int64 // remember for the footer layout

	// read mode
	fst       *vellum.FST
	fstBuf    *bytes.Buffer
	fstOffset int64
}

type InvertedIndexWriter[V constraints.Ordered] interface {
	io.Closer // flush FST
	// Put must be called so terms are sorted, values must also be sorted beforehand
	Put(term string, values []V) error
}

type InvertedIndexReader[V constraints.Ordered] interface {
	ReadTerms() (lezhnev74.Iterator[string], error)
	// ReadValues returns sorted iterator
	ReadValues(terms []string, min V, max V) (lezhnev74.Iterator[V], error)
	io.Closer
}

func (i *InvertedIndex[V]) Close() error {
	if i.fstBuilder != nil { // only for writer mode

		err := i.fstBuilder.Close()
		if err != nil {
			return fmt.Errorf("fst: %w", err)
		}

		allValues := i.getAllTermValues()

		termBitmaps := i.mapTermValuesToBitmaps(allValues)
		i.termsValues = nil // free up

		err = i.writeAllValues(allValues)
		if err != nil {
			return fmt.Errorf("writing all values failed: %w", err)
		}
		allValues = nil // free up

		err = i.writeTermsBitmapsAndUpdateFST(termBitmaps)
		if err != nil {
			return fmt.Errorf("writing bitmaps failed: %w", err)
		}

		// write FST
		fstL, err := i.file.Write(i.fstBuf.Bytes())
		if err != nil {
			return fmt.Errorf("fst: failed writing: %w", err)
		}

		binary.BigEndian.PutUint32(i.buf4, uint32(fstL))
		_, err = i.file.Write(i.buf4)
		if err != nil {
			return fmt.Errorf("fst: failed writing size: %w", err)
		}
		i.fstBuf = nil // free up

		// write the footer
		binary.BigEndian.PutUint32(i.buf4, i.segmentSize)
		_, err = i.file.Write(i.buf4)
		if err != nil {
			return fmt.Errorf("failed writing segment size: %w", err)
		}

		buf8 := make([]byte, 8)
		binary.BigEndian.PutUint64(buf8, uint64(i.valuesIndexOffset))
		_, err = i.file.Write(i.buf4)
		if err != nil {
			return fmt.Errorf("failed writing index offset: %w", err)
		}

		return nil
	}

	i.closeFile.Do(func() { i.file.Close() })
	return nil
}

// Put remembers all terms and its values, actual writing is delayed until Close()
func (i *InvertedIndex[V]) Put(term string, values []V) error {

	if i.lastTerm == term {
		return ErrDuplicateTerm
	}
	i.lastTerm = term

	err := i.fstBuilder.Insert([]byte(term), uint64(len(i.termsValues)))
	if err != nil {
		return err
	}

	i.termsValues = append(i.termsValues, values)

	return nil
}

func (i *InvertedIndex[V]) ReadValues(terms []string, minVal V, maxVal V) (lezhnev74.Iterator[V], error) {

	if len(terms) == 0 {
		return lezhnev74.NewSliceIterator([]V{}), nil
	}

	// Note 1:
	// for every term makes a sorted iterator for values
	// joins all iterators to a unique selection tree
	// effectively all values are sorted now

	// Note 2:
	// To find one term's values boundary we need to use the offset from FST as the start
	// and the next term's offset as the end.

	// make iterators for term's values
	slices.Sort(terms)
	fstIt, err := i.fst.Iterator([]byte(terms[0]), nil)
	if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
		return nil, fmt.Errorf("fst: %w", err)
	} else if errors.Is(err, vellum.ErrIteratorDone) {
		return lezhnev74.NewSliceIterator([]V{}), nil
	}

	var (
		offset, nextOffset uint64
		existingTerm       []byte
	)
	iterators := make([]lezhnev74.Iterator[V], 0)

	for _, term := range terms {

		// figure out term values file region
		err = fstIt.Seek([]byte(term))
		existingTerm, offset = fstIt.Current()
		if slices.Compare(existingTerm, []byte(term)) != 0 {
			continue // term is not in the index
		}

		err = fstIt.Next()
		if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
			return nil, fmt.Errorf("fst: %w", err)
		} else if errors.Is(err, vellum.ErrIteratorDone) {
			nextOffset = uint64(i.fstOffset)
		} else {
			_, nextOffset = fstIt.Current()
		}

		termFetchFunc, err := i.makeTermValuesFetchFunc(term, offset, nextOffset, minVal, maxVal)
		if err != nil {
			return nil, fmt.Errorf("failed reading term values: %w", err)
		}

		closeIterator := func() error {
			i.closeFile.Do(func() { i.file.Close() })
			return nil
		}

		iterators = append(iterators, lezhnev74.NewDynamicSliceIterator(termFetchFunc, closeIterator))
	}

	// make a selection tree
	tree := lezhnev74.NewSliceIterator([]V{})
	for _, it := range iterators {
		tree = lezhnev74.NewUniqueSelectingIterator(tree, it, lezhnev74.OrderedCmpFunc[V])
	}

	return tree, nil
}
func (i *InvertedIndex[V]) ReadTerms() (lezhnev74.Iterator[string], error) {
	it, err := i.fst.Iterator(nil, nil)
	if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
		return nil, err
	} else if errors.Is(err, vellum.ErrIteratorDone) {
		return lezhnev74.NewSliceIterator([]string{}), nil
	}

	var lastErr error
	itWrap := lezhnev74.NewCallbackIterator(
		func() (v string, err error) {
			if lastErr != nil {
				return "", lastErr
			}
			term, _ := it.Current()
			if len(term) == 0 {
				return "", lezhnev74.ClosedIterator
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

func (i *InvertedIndex[V]) readFooter() error {
	// todo: use mmap to read the footer of the file: index,indexsize,fst,fstsize (remove seeks)

	buf := make([]byte, 4096)
	sizeLen := 4 // uint32 size
	var fstLen int

	// read the end of the file for parsing footer
	fstat, err := i.file.Stat()
	if err != nil {
		return err
	}

	if fstat.Size() < int64(sizeLen) {
		return fmt.Errorf("the file size is too small (%d bytes), probably corrupted", fstat.Size())
	}

	minRead := min(fstat.Size(), int64(len(buf)))
	_, err = i.file.Seek(-minRead, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("fst: %w", err)
	}

	n, err := i.file.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read footer length: %w", err)
	}

	// extract index, fst lengths
	fstLen = int(binary.BigEndian.Uint32(buf[n-sizeLen:]))
	i.fstOffset = fstat.Size() - int64(sizeLen) - int64(fstLen)

	// read fst body
	if fstLen > n-sizeLen { // do we need to read more bytes?
		_, err := i.file.Seek(-int64(fstLen+sizeLen), io.SeekEnd)
		if err != nil {
			return err
		}

		buf = make([]byte, fstLen)
		_, err = i.file.Read(buf)
		if err != nil {
			return err
		}
	} else {
		buf = buf[n-sizeLen-fstLen : n-sizeLen]
	}

	i.fst, err = vellum.Load(buf[len(buf)-fstLen:]) // todo: use file for mmap io
	if err != nil {
		return fmt.Errorf("fst: load failed: %w", err)
	}

	return nil
}

func (i *InvertedIndex[V]) makeTermValuesFetchFunc(
	term string,
	offset, nextOffset uint64,
	minVal, maxVal V,
) (func() ([]V, error), error) {
	var indexLen, n int
	sizeLen := 4
	buf := make([]byte, 4096)
	valuesLen := nextOffset - offset
	var segments []segmentIndexEntry[V]

	// read segment index
	readFrom := int64(-1)
	if int(valuesLen) < len(buf) {
		// whole region fits in the buffer
		buf = buf[:valuesLen]
		readFrom = int64(offset)
	} else {
		// buffer only partially covers the region
		readFrom = int64(nextOffset) - int64(len(buf))
	}

	_, err := i.file.Seek(readFrom, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("reading values index: %w", err)
	}
	n, err = i.file.Read(buf)
	if err != nil {
		return nil, fmt.Errorf("reading values index: %w", err)
	}

	indexLen = int(binary.BigEndian.Uint32(buf[n-sizeLen:]))
	indexOffset := int64(nextOffset) - int64(sizeLen) - int64(indexLen)

	if indexLen > n-sizeLen { // do we need to read more bytes?
		_, err = i.file.Seek(indexOffset, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("reading values index: %w", err)
		}

		buf = make([]byte, indexLen)
		_, err = i.file.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("reading values index: %w", err)
		}
	} else {
		buf = buf[n-sizeLen-indexLen : n-sizeLen]
	}

	segments, err = decodeSegmentsIndex(buf, i.unserializeSegment)
	if err != nil {
		return nil, fmt.Errorf("decoding values index: %w", err)
	}

	// Make a term iterator that scans through segments sequentially
	// makeTermFetchFunc returns a function that can be used in an iterator,
	// upon calling the func it will return slices of sorted term's values
	makeTermFetchFunc := func(term string) func() ([]V, error) {
		var (
			segmentLen int64
			minS, maxS V
		)
		segmentBuf := make([]byte, 4096)
		si := 0

		var retFunc func() ([]V, error)
		retFunc = func() ([]V, error) {
			// validate the current segment
			if si == len(segments) {
				return nil, lezhnev74.EmptyIterator
			}

			if segments[si].Min > maxVal {
				return nil, lezhnev74.EmptyIterator // no further segments are good
			}

			minS = segments[si].Min
			maxS = maxVal
			if si < len(segments)-1 {
				maxS = segments[si+1].Min
			}
			if minVal > maxS || maxVal < minS {
				return nil, lezhnev74.EmptyIterator // no further segments are good
			}

			s := &segments[si]

			// read the segment
			_, err = i.file.Seek(s.Offset, io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			if si == len(segments)-1 {
				segmentLen = indexOffset - s.Offset // the last segment
			} else {
				segmentLen = segments[si+1].Offset - s.Offset
			}

			if int64(cap(segmentBuf)) < segmentLen {
				segmentBuf = make([]byte, segmentLen)
			} else {
				segmentBuf = segmentBuf[:segmentLen]
			}

			_, err = i.file.Read(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			values, err := i.unserializeSegment(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("values: decompress fail: %w", err)
			}

			si++

			// finally filter values in place with respect to the min/max scope
			k := 0
			for _, v := range values {
				if v < minVal || v > maxVal {
					continue
				}
				values[k] = v
				k++
			}
			values = values[:k]

			if len(values) == 0 {
				// filtering revealed that this segment has no matching values,
				// continue to the next segment:
				return retFunc()
			}

			return values, nil
		}

		return retFunc
	}

	return makeTermFetchFunc(term), nil
}

func (i *InvertedIndex[V]) getAllTermValues() []V {
	totalNum := 0
	for _, tv := range i.termsValues {
		totalNum += len(tv)
	}

	allValues := make([]V, totalNum/2) // to avoid many allocations, start with 50% of total
	for _, tv := range i.termsValues {
		for _, v := range tv {
			if slices.Contains(allValues, v) {
				continue
			}
			allValues = append(allValues, v)
		}
	}
	slices.Sort(allValues)

	return allValues
}

func (i *InvertedIndex[V]) mapTermValuesToBitmaps(allValues []V) []*roaring.Bitmap {
	tb := make([]*roaring.Bitmap, 0, len(i.termsValues)) // bitmaps per term
	pv := make([]uint32, 0)                              // term value positions for the bitmap

	for _, tv := range i.termsValues {
		pv = pv[:0]
		for _, v := range tv {
			p, _ := slices.BinarySearch(allValues, v)
			pv = append(pv, uint32(p))
		}
		b := roaring.BitmapOf(pv...)
		tb = append(tb, b)
	}

	return tb
}

func (i *InvertedIndex[V]) writeAllValues(values []V) error {
	index := make([]segmentIndexEntry[V], 0, len(values)/int(i.segmentSize)+1)

	// write segments
	for k := 0; k < len(values); k += int(i.segmentSize) {

		segmentOffset := i.filePos
		segment := values[k:min(len(values), k+int(i.segmentSize))]
		sbuf, err := i.serializeSegment(segment) // todo: wasted allocation?
		if err != nil {
			return fmt.Errorf("serializing values segment failed: %w", err)
		}

		sl, err := i.file.Write(sbuf)
		if err != nil {
			return err
		}
		i.filePos += int64(sl)

		index = append(index, segmentIndexEntry[V]{
			Offset: segmentOffset,
			Min:    segment[0],
		})
	}

	// write segments index
	indexBuf, err := encodeSegmentsIndex(index, i.serializeSegment)
	if err != nil {
		return fmt.Errorf("encoding segments index failed: %w", err)
	}

	n, err := i.file.Write(indexBuf)
	if err != nil {
		return fmt.Errorf("failed writing values index: %w", err)
	}
	i.valuesIndexOffset = i.filePos
	i.filePos += int64(n)

	binary.BigEndian.PutUint32(i.buf4, uint32(n))
	n, err = i.file.Write(i.buf4)
	if err != nil {
		return fmt.Errorf("failed writing values index size for: %w", err)
	}
	i.filePos += int64(n)

	return nil
}

func (i *InvertedIndex[V]) writeTermsBitmapsAndUpdateFST(bitmaps []*roaring.Bitmap) error {
	// use our existing fst to iterate through terms in the same order as they were ingested
	fst, err := vellum.Load(i.fstBuf.Bytes())
	if err != nil {
		return fmt.Errorf("fst read failed: %w", err)
	}

	termsIt, err := fst.Iterator(nil, nil)
	if err != nil {
		return fmt.Errorf("fst iterator failed: %w", err)
	}

	// here we create a new FST with actual bitmap offsets
	// since we can't update the existing FST build with term numbers
	i.fstBuilder, err = vellum.New(i.fstBuf, nil)
	var n int64
	for {
		t, tn := termsIt.Current() // the value is the index of a term
		err = i.fstBuilder.Insert(t, uint64(i.filePos))
		if err != nil {
			return err
		}

		// write the bitmap
		n, err = bitmaps[tn].WriteTo(i.file) // the index of a bitmap is the same as the term in FST
		if err != nil {
			return err
		}
		i.filePos += n

		err = termsIt.Next()
		if err != nil && errors.Is(err, vellum.ErrIteratorDone) {
			break
		} else if err != nil {
			return fmt.Errorf("fst iteration failed: %w", err)
		}
	}
	err = i.fstBuilder.Close()
	if err != nil {
		return err
	}

	return nil
}

func NewInvertedIndexUnit[V constraints.Ordered](
	filename string,
	segmentSize uint32,
	serializeValues func([]V) ([]byte, error),
	unserializeValues func([]byte) ([]V, error),
) (InvertedIndexWriter[V], error) {
	if segmentSize < 1 {
		return nil, fmt.Errorf("the segment size is too small")
	}

	var err error
	iiw := &InvertedIndex[V]{
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		buf8:               make([]byte, 8),
		serializeSegment:   serializeValues,
		unserializeSegment: unserializeValues,
		segmentSize:        segmentSize,
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
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	i := &InvertedIndex[V]{
		file:               f,
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		unserializeSegment: unserializeValues,
		cmp:                lezhnev74.OrderedCmpFunc[V],
	}

	err = i.readFooter()
	if err != nil {
		return nil, err
	}

	return i, nil
}
