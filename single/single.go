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
	"golang.org/x/exp/mmap"
	"golang.org/x/exp/slices"
	"io"
	"os"
	"sync"
)

/**

		File Layout:

                            ┌─────*──────┬──8───┬───────4───────┬────4─────┬────────8────────┐
                            │MinMaxValues│FSTLen│MinMaxValuesLen│SegmentLen│ValuesIndexOffset│
                            └────────────┴──────┴───────────────┴──────────┴─────────────────┘
                             \          ____________________________________________________/
                              \        /
            ┌──*───┬───*───┬─*─┬──*───┐
            │Values│Bitmaps│FST│Footer│
            └──────┴───────┴───┴──────┘
          /         \__________________________
         |                                     \
         ┌───*────┬─────┬───*────┬───8────┬──*──┐
         │Segment1│ ... │SegmentN│IndexLen│Index│
         └────────┴─────┴────────┴────────┴─────┘

*/

var (
	ErrDuplicateTerm = fmt.Errorf("duplicate term inserted")
	ErrEmptyIndex    = fmt.Errorf("no terms inserted")
)

// InvertedIndex is a single index piece consists of terms FST + sorted values for each term
// resembles a sorted map[string][]int
// NOT CONCURRENT
type InvertedIndex[V cmp.Ordered] struct {
	closeFile      sync.Once
	filePos        int64
	buf4, buf8     []byte // len buf
	minVal, maxVal V
	/*
		segmentSize specifies the number of values in a segment.
		The smaller the segment the less read we hopefully do,
		However too small size increases the index and fseeks.
	*/
	segmentSize uint32
	// Generic Values --------------------------------------------------
	/*
		serializeSegment is user-provided function to compress values
		since Value is generic.
	*/
	serializeSegment func(items []V) ([]byte, error)

	unserializeSegment func(data []byte) (items []V, err error)
	cmp                func(a, b V) int // -1,0,1 to impose order on values

	// Writer-mode -----------------------------------------------------
	file *os.File
	// Accumulates data and writes our on Close()
	fstBuilder   *vellum.Builder
	bytesWritten uint32
	// lastTerm is used to prevent term duplicates, ingested terms must be sorted and unique
	lastTerm string
	/*
		termsValues are used to keep ingested data in memory, upon Close() actual writing is happening.
		Writing makes a unique list of all values and generates bitmaps for each term in the index.
		That allows to greatly reduce the index file size.
	*/
	termsValues []termValues[V] // temporary values for each term

	// Reader-mode -----------------------------------------------------
	// Reads immutable data in the file
	mmapFile *mmap.ReaderAt
	// fst allows to compress terms in the index file
	fst                    *vellum.FST
	fstBuf                 *bytes.Buffer
	fstOffset, indexOffset int64
	// bitmaps cache stores all bitmaps indexed by file offsets
	bitmaps       map[int64]*roaring.Bitmap
	segmentsIndex []segmentIndexEntry[V]
}

type termValues[V constraints.Ordered] struct {
	term   string
	values []V
}

type InvertedIndexWriter[V constraints.Ordered] interface {
	io.Closer // flush FST
	// Put must be called so terms are sorted, values must also be sorted beforehand
	Put(term string, values []V) error
}

type InvertedIndexReader[V constraints.Ordered] interface {
	// ReadTerms returns sorted iterator
	ReadTerms() (go_iterators.Iterator[string], error)
	// ReadValues returns sorted iterator
	ReadValues(terms []string, min V, max V) (go_iterators.Iterator[V], error)
	ReadAllValues(terms []string) (go_iterators.Iterator[V], error)
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

func (i *InvertedIndex[V]) Len() int64 { return i.filePos }

// Put remembers all terms and its values, actual writing is delayed until Close()
func (i *InvertedIndex[V]) Put(term string, values []V) error {

	tv := termValues[V]{term, values}
	j, ok := slices.BinarySearchFunc(i.termsValues, tv, func(a, b termValues[V]) int {
		return cmp.Compare(a.term, b.term)
	})
	if ok {
		return ErrDuplicateTerm
	}

	i.termsValues = slicePutAt(i.termsValues, j, tv)

	return nil
}

func (i *InvertedIndex[V]) ReadValues(terms []string, minVal V, maxVal V) (go_iterators.Iterator[V], error) {

	if len(terms) == 0 {
		return go_iterators.NewSliceIterator([]V{}), nil
	}

	// Note:
	// Read all terms bitmaps and join into one.
	// Use it to select segments for reading.

	bs, err := i.readTermsBitmaps(terms)
	if err != nil {
		return nil, fmt.Errorf("bitmaps: %w", err)
	}
	if len(bs) == 0 {
		return go_iterators.NewSliceIterator([]V{}), nil
	}

	b := bs[0]
	for j := 1; j < len(bs); j++ {
		b.Or(bs[j])
	}

	segmentsIndex := i.preselectSegments(b, minVal, maxVal)

	valuesFetchFunc, err := i.makeSegmentsFetchFunc(segmentsIndex, b, minVal, maxVal)
	if err != nil {
		return nil, fmt.Errorf("failed reading term values: %w", err)
	}

	closeIterator := func() error {
		i.closeFile.Do(func() { i.file.Close() })
		return nil
	}
	it := go_iterators.NewDynamicSliceIterator(valuesFetchFunc, closeIterator)

	return it, nil
}

func (i *InvertedIndex[V]) ReadAllValues(terms []string) (go_iterators.Iterator[V], error) {
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

// sliceSortUnique removes duplicates in place, returns sorted values
// also see slices.Compact
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

// slicePutAt is an efficient insertion function that avoid unnecessary allocations
func slicePutAt[V any](dst []V, pos int, v V) []V {
	dst = append(dst, v) // could grow here
	copy(dst[pos+1:], dst[pos:])
	dst[pos] = v
	return dst
}

func (i *InvertedIndex[V]) mapTermValuesToBitmaps(allValues []V) []*roaring.Bitmap {
	tb := make([]*roaring.Bitmap, 0, len(i.termsValues)) // bitmaps per term
	pv := make([]uint32, 0)                              // term value positions for the bitmap

	for _, tv := range i.termsValues {
		pv = pv[:0]
		for _, v := range tv.values {
			p, _ := slices.BinarySearch(allValues, v)
			pv = append(pv, uint32(p))
		}
		b := roaring.BitmapOf(pv...)
		tb = append(tb, b)
	}

	return tb
}

func (i *InvertedIndex[V]) writeAllValues(values []V) (valuesIndexOffset int64, err error) {
	var n int
	index := make([]segmentIndexEntry[V], 0, len(values)/int(i.segmentSize)+1)

	// write segments
	for k := 0; k < len(values); k += int(i.segmentSize) {

		segmentOffset := i.filePos
		segment := values[k:min(len(values), k+int(i.segmentSize))]
		sbuf, err := i.serializeSegment(segment) // todo: wasted allocation?
		if err != nil {
			return 0, fmt.Errorf("serializing values segment failed: %w", err)
		}

		sl, err := i.file.Write(sbuf)
		if err != nil {
			return 0, err
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
		return 0, fmt.Errorf("encoding segments index failed: %w", err)
	}

	valuesIndexOffset = i.filePos

	binary.BigEndian.PutUint64(i.buf8, uint64(len(indexBuf)))
	n, err = i.file.Write(i.buf8)
	if err != nil {
		return 0, fmt.Errorf("failed writing values index size for: %w", err)
	}
	i.filePos += int64(n)

	n, err = i.file.Write(indexBuf)
	if err != nil {
		return 0, fmt.Errorf("failed writing values index: %w", err)
	}
	i.filePos += int64(n)

	return
}

func (i *InvertedIndex[V]) writeTermsBitmapsAndUpdateFST(bitmaps []*roaring.Bitmap) error {

	var (
		err error
		n   int64
	)

	// here we create a new FST with actual bitmap offsets
	// since we can't update the existing FST build with term numbers
	i.fstBuilder, err = vellum.New(i.fstBuf, nil)
	for tn, tv := range i.termsValues {
		err = i.fstBuilder.Insert([]byte(tv.term), uint64(i.filePos))
		if err != nil {
			return err
		}

		// write the bitmap
		n, err = bitmaps[tn].WriteTo(i.file) // the index of a bitmap is the same as the term in FST
		if err != nil {
			return err
		}
		i.filePos += n
	}

	err = i.fstBuilder.Close()
	if err != nil {
		return err
	}

	return nil
}

func (i *InvertedIndex[V]) writeFooter(valuesIndexOffset int64, fstL int) error {

	values := []V{i.minVal, i.maxVal}
	vBuf, err := i.serializeSegment(values)
	if err != nil {
		return fmt.Errorf("footer: failed compressing min/max values: %w", err)
	}

	n, err := i.file.Write(vBuf)
	if err != nil {
		return fmt.Errorf("footer: failed writing min/max values: %w", err)
	}
	i.filePos += int64(n)

	binary.BigEndian.PutUint64(i.buf8, uint64(fstL))
	n, err = i.file.Write(i.buf8)
	if err != nil {
		return fmt.Errorf("fst: failed writing size: %w", err)
	}
	i.filePos += int64(n)

	binary.BigEndian.PutUint32(i.buf4, uint32(len(vBuf)))
	n, err = i.file.Write(i.buf4)
	if err != nil {
		return fmt.Errorf("footer: failed writing min/max values size: %w", err)
	}
	i.filePos += int64(n)

	binary.BigEndian.PutUint32(i.buf4, i.segmentSize)
	n, err = i.file.Write(i.buf4)
	if err != nil {
		return fmt.Errorf("footer: failed writing segment size: %w", err)
	}
	i.filePos += int64(n)

	binary.BigEndian.PutUint64(i.buf8, uint64(valuesIndexOffset))
	n, err = i.file.Write(i.buf8)
	if err != nil {
		return fmt.Errorf("footer: failed writing index offset: %w", err)
	}
	i.filePos += int64(n)

	return nil
}

// getAllTermValues makes a single sorted slice of all unique values attached to terms
func (i *InvertedIndex[V]) getAllTermValues() []V {
	totalNum := 0
	for _, tv := range i.termsValues {
		totalNum += len(tv.values)
	}

	allValues := make([]V, 0, totalNum)
	for _, tv := range i.termsValues {
		allValues = append(allValues, tv.values...)
	}

	return sliceSortUnique(allValues)
}

// write flushes all in-memory data to the file
func (i *InvertedIndex[V]) write() error {

	allValues := i.getAllTermValues()

	if len(allValues) == 0 {
		var empty V
		i.minVal = empty
		i.maxVal = empty
	} else {
		i.minVal = allValues[0]
		i.maxVal = allValues[len(allValues)-1]
	}

	termBitmaps := i.mapTermValuesToBitmaps(allValues)

	valuesIndexOffset, err := i.writeAllValues(allValues)
	if err != nil {
		return fmt.Errorf("writing all values failed: %w", err)
	}
	allValues = nil // free up

	err = i.writeTermsBitmapsAndUpdateFST(termBitmaps)
	if err != nil {
		return fmt.Errorf("writing bitmaps failed: %w", err)
	}
	i.termsValues = nil // free up

	// write FST
	fstL, err := i.file.Write(i.fstBuf.Bytes())
	if err != nil {
		return fmt.Errorf("fst: failed writing: %w", err)
	}
	i.fstBuf = nil // free up
	i.filePos += int64(fstL)

	// write the footer
	return i.writeFooter(valuesIndexOffset, fstL)
}

func (i *InvertedIndex[V]) readFooter() (
	fst *vellum.FST,
	segmentSize int64,
	indexOffset int64,
	fstOffset int64,
	minValue V,
	maxValue V,
	err error,
) {

	buf := make([]byte, 4096)
	fstLenSize := int64(8)
	minMaxSize := int64(4)
	segSize := int64(4)
	indexOffsetSize := int64(8)
	totalSizes := fstLenSize + minMaxSize + segSize + indexOffsetSize

	var (
		fstLen int64
		mmBuf  []byte
	)

	// read the end of the file for parsing footer
	fileSize := int64(i.mmapFile.Len())

	if fileSize <= totalSizes {
		// This is a naive protection, works for empty files, does not cover all corruptions.
		// For proper file consistency it should check a hashsum.
		err = fmt.Errorf("the file size is too small (%d bytes), probably corrupted", fileSize)
		return
	}

	p := min(fileSize, int64(len(buf)))
	offset := fileSize - p

	n, err := i.mmapFile.ReadAt(buf, int64(offset))
	if err != nil && !errors.Is(err, io.EOF) {
		err = fmt.Errorf("read footer length: %w", err)
		return
	}

	// extract footer numbers
	indexOffset = int64(binary.BigEndian.Uint64(buf[int64(n)-indexOffsetSize:]))

	segmentSize = int64(binary.BigEndian.Uint32(buf[int64(n)-indexOffsetSize-segSize : int64(n)-indexOffsetSize]))

	minMaxLen := int64(binary.BigEndian.Uint32(buf[int64(n)-indexOffsetSize-segSize-minMaxSize : int64(n)-indexOffsetSize-segSize]))

	fstLen = int64(binary.BigEndian.Uint64(buf[int64(n)-indexOffsetSize-segSize-minMaxSize-fstLenSize : int64(n)-indexOffsetSize-segSize-minMaxSize]))

	fstOffset = fileSize - fstLen - fstLenSize - segSize - indexOffsetSize - minMaxSize - minMaxLen

	// read fst body
	if fstLen > int64(n)-minMaxLen-totalSizes { // do we need to read more bytes?
		offset = fileSize - (fstLen + minMaxLen + totalSizes)
		buf = make([]byte, fstLen)
		_, err = i.mmapFile.ReadAt(buf, offset)
		if err != nil {
			return
		}
	} else {
		// if FST is in the buf already, that means minMax values are there too
		mmBuf = buf[n-int(minMaxLen+totalSizes) : int64(n)-totalSizes]
		buf = buf[n-int(fstLen+minMaxLen+totalSizes) : int64(n)-totalSizes-minMaxLen]
	}

	fst, err = vellum.Load(buf)
	if err != nil {
		err = fmt.Errorf("fst: load failed: %w", err)
		return
	}

	// read min-max values
	if minMaxLen > int64(len(mmBuf)) { // do we need to read more bytes?
		offset = fileSize - (minMaxLen + totalSizes)
		mmBuf = make([]byte, minMaxLen)
		_, err = i.mmapFile.ReadAt(mmBuf, offset)
		if err != nil {
			return
		}
	}

	values, err := i.unserializeSegment(mmBuf)
	if err != nil {
		return
	}
	minValue, maxValue = values[0], values[1]

	return
}

// readTermsBitmaps returns all bitmaps associated with the given terms
func (i *InvertedIndex[V]) readTermsBitmaps(terms []string) ([]*roaring.Bitmap, error) {
	slices.Sort(terms)

	var (
		offset uint64
		err    error
		ok     bool
	)

	bitmaps := make([]*roaring.Bitmap, 0, len(terms))

	for _, term := range terms {

		// figure out term's bitmap file region
		offset, ok, err = i.fst.Get([]byte(term))
		if err != nil && !errors.Is(err, vellum.ErrIteratorDone) {
			return nil, fmt.Errorf("fst seek term: %w", err)
		} else if errors.Is(err, vellum.ErrIteratorDone) {
			break // no further terms will match
		} else if !ok {
			continue
		}

		bitmaps = append(bitmaps, i.bitmaps[int64(offset)])
	}

	return bitmaps, nil
}

func (i *InvertedIndex[V]) readValuesIndex() (index []segmentIndexEntry[V], err error) {
	buf := make([]byte, 100)
	n, err := i.mmapFile.ReadAt(buf, i.indexOffset)
	if err != nil {
		return nil, fmt.Errorf("reading index len: %w", err)
	}

	indexLen := int64(binary.BigEndian.Uint64(buf[:8]))

	if int64(n) < (indexLen + 8) {
		// index is only partially in the buf
		moreSpace := indexLen + 8 - int64(n)
		buf = append(buf, make([]byte, moreSpace)...)
		_, err = i.mmapFile.ReadAt(buf, i.indexOffset)
		if err != nil {
			return nil, fmt.Errorf("reading index len: %w", err)
		}
	}
	buf = buf[8 : indexLen+8] // skip the size

	index, err = decodeSegmentsIndex(buf, i.unserializeSegment)
	if err != nil {
		return nil, fmt.Errorf("decoding values index: %w", err)
	}

	// put each segment's len for further simpler reading as (offset + len)
	for j, s := range index {
		if j+1 == len(index) {
			index[j].len = i.indexOffset - s.Offset
		} else {
			index[j].len = index[j+1].Offset - s.Offset
		}
	}

	return
}

func (i *InvertedIndex[V]) preselectSegments(b *roaring.Bitmap, minVal, maxVal V) []segmentIndexEntry[V] {
	// Note: preselection returns only segments that potentially can contain relevant values
	// Based on min/max values and the terms bitmap

	index := i.segmentsIndex // the index was pre-loaded

	// Filter out segments based on bitmap
	k := 0
	lastMatchedSegment := -1
	b.Iterate(func(v uint32) bool {
		matchedSegment := v / i.segmentSize            // int division
		if lastMatchedSegment == int(matchedSegment) { // this is to process each matched segment only once
			return true
		}
		lastMatchedSegment = int(matchedSegment)

		index[matchedSegment].startNum = int(matchedSegment * i.segmentSize)
		index[k] = index[matchedSegment]
		k++
		return true
	})

	index = index[:k]

	// Filter out based on min/max values
	var minSegV, maxSegV V
	k = 0
	for j := 0; j < len(index); j++ {
		minSegV = index[j].Min
		maxSegV = maxVal
		if (j + 1) < len(index) {
			maxSegV = index[j+1].Min
		}
		if minVal > maxSegV || maxVal < minSegV {
			continue
		}
		index[k] = index[j]
		k++
	}
	index = index[:k]

	return index
}

func (i *InvertedIndex[V]) makeSegmentsFetchFunc(
	segments []segmentIndexEntry[V],
	b *roaring.Bitmap,
	minVal V,
	maxVal V,
) (func() ([]V, error), error) {
	// Make an iterator that scans through segments sequentially
	// Returns a function that can be used in a slice fetching iterator,
	// upon calling the func it will return slices of sorted values
	makeFetchFunc := func() func() ([]V, error) {
		segmentBuf := make([]byte, 4096)
		si := 0

		var retFunc func() ([]V, error)
		retFunc = func() ([]V, error) {
			var err error

			// validate the current segment
			if si == len(segments) {
				return nil, go_iterators.EmptyIterator
			}

			s := &segments[si]

			// read the segment
			if int64(cap(segmentBuf)) < s.len {
				segmentBuf = make([]byte, s.len)
			} else {
				segmentBuf = segmentBuf[:s.len]
			}

			_, err = i.mmapFile.ReadAt(segmentBuf, s.Offset)
			if err != nil {
				return nil, fmt.Errorf("read values segment failed: %w", err)
			}

			values, err := i.unserializeSegment(segmentBuf)
			if err != nil {
				return nil, fmt.Errorf("values: decompress fail: %w", err)
			}

			// filter out segment values based on the bitmap
			segmentBitmap := roaring.New()
			segmentBitmap.AddRange(uint64(s.startNum), uint64(s.startNum)+uint64(i.segmentSize))
			segmentBitmap.And(b)

			sit := segmentBitmap.Iterator()
			k := 0
			for sit.HasNext() {
				vi := sit.Next()
				values[k] = values[vi-uint32(s.startNum)]
				k++
			}
			values = values[:k]

			// finally filter out values in place with respect to the min/max scope
			k = 0
			for _, v := range values {
				if v < minVal || v > maxVal {
					continue
				}
				values[k] = v
				k++
			}
			values = values[:k]

			si++

			if len(values) == 0 {
				// filtering revealed that this segment has no matching values,
				// continue to the next segment:
				return retFunc()
			}

			return values, nil
		}

		return retFunc
	}

	return makeFetchFunc(), nil
}

// readBitmaps populates internal cache with all bitmaps in the file
func (i *InvertedIndex[V]) readBitmaps() error {

	var (
		err error
		n   int
	)

	if i.bitmaps != nil { // already cached
		return nil
	}
	i.bitmaps = make(map[int64]*roaring.Bitmap)

	mTerm, _ := i.fst.GetMinKey()
	fileBitmapsOffset, _, _ := i.fst.Get(mTerm)
	bitmapsLen := i.fstOffset - int64(fileBitmapsOffset)

	// Load all bitmaps bytes into memory
	bitmapsMem := make([]byte, bitmapsLen)
	n, err = i.mmapFile.ReadAt(bitmapsMem, int64(fileBitmapsOffset))
	if err != nil {
		return err
	} else if int64(n) < bitmapsLen {
		return fmt.Errorf("bitmap read: file is corrupted")
	}

	// now extract bitmaps for each term in FST
	it, err := i.fst.Iterator(nil, nil)
	if err != nil {
		return err
	}
	for err == nil {
		_, termBitmapOffset := it.Current()
		termBitmapBufferOffset := termBitmapOffset - fileBitmapsOffset // adapt to buffer boundaries

		termBitmap := roaring.New()
		_, err = termBitmap.ReadFrom(bytes.NewBuffer(bitmapsMem[termBitmapBufferOffset:]))
		if err != nil {
			return fmt.Errorf("parse bitmap: %w", err)
		}
		i.bitmaps[int64(termBitmapOffset)] = termBitmap

		err = it.Next()
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
	f, err := mmap.Open(file)
	if err != nil {
		return nil, err
	}

	i := &InvertedIndex[V]{
		mmapFile:           f,
		fstBuf:             new(bytes.Buffer),
		buf4:               make([]byte, 4),
		unserializeSegment: unserializeValues,
		cmp:                cmp.Compare[V],
	}

	fst, segmSize, indexOffset, fstOffset, minValue, maxValue, err := i.readFooter()
	if err != nil {
		return nil, err
	}

	i.fst = fst
	i.segmentSize = uint32(segmSize)
	i.fstOffset = fstOffset
	i.indexOffset = indexOffset
	i.minVal = minValue
	i.maxVal = maxValue

	// to reduce seeks and reads, it is a good idea to load all bitmaps into memory once,
	// then use that cache for querying bitmaps.
	err = i.readBitmaps()
	if err != nil {
		return nil, err
	}

	i.segmentsIndex, err = i.readValuesIndex()
	if err != nil {
		return nil, fmt.Errorf("read values index: %w", err)
	}

	return i, nil
}
