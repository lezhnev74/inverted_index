package single

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
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

			   ┌────────────┬───────┬────────────┬───┬──────┐
			   │term1 values│  ...  │termN values│FST│fstLEN│
			   └────────────┴───────┴────────────┴───┴──────┘
			  /                    \
			 /                      \
			/                        \
		   /                          \
		 ┌─────┬────────┬─────┬────────┐
		 │index│segment1│ ... │segmentN│
		 └─────┴────────┴─────┴────────┘
		/       \
	   /         \
	  /           \
	 /             \
	┌────────┬─────┐
	│indexLEN│index│
	└────────┴─────┘

*/

// InvertedIndex is a single index piece consists of terms FST + sorted values for each term
// resembles a sorted map[string][]int
// NOT CONCURRENT
type InvertedIndex[V cmp.Ordered] struct {
	file    *os.File
	filePos int64
	lbuf    []byte // len buf

	// Values
	serialize   func(items []V) ([]byte, error)
	unserialize func(data []byte) (items []V, err error)
	cmp         func(a, b V) int // -1,0,1 to impose order on values

	// write mode
	fstBuilder   *vellum.Builder
	bytesWritten uint32
	segmentSize  int // in number of values in a segment

	// read mode
	fst       *vellum.FST
	fstBuf    *bytes.Buffer
	fstOffset int64
}

type segmentIndexEntry[V constraints.Ordered] struct {
	Offset, Len int64
	Min         V
}

func compressGob[T any](items []T) ([]byte, error) {
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	err := enc.Encode(items)
	return w.Bytes(), err
}

func decompressGob[T any](data []byte) (items []T, err error) {
	w := bytes.NewBuffer(data)
	enc := gob.NewDecoder(w)
	err = enc.Decode(&items)
	return
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
}

func (i *InvertedIndex[V]) Close() error {
	// write FST
	err := i.fstBuilder.Close()
	if err != nil {
		return fmt.Errorf("fst: %w", err)
	}

	fstL, err := i.file.Write(i.fstBuf.Bytes())
	if err != nil {
		return fmt.Errorf("fst: failed writing: %w", err)
	}

	// write fst len
	binary.BigEndian.PutUint32(i.lbuf, uint32(fstL))
	_, err = i.file.Write(i.lbuf)
	if err != nil {
		return fmt.Errorf("fst: failed writing size: %w", err)
	}
	i.fstBuf = nil

	return i.file.Close()
}

// Put writes our values to the file first, then update FST
// split all values into segments to optimize reading
func (i *InvertedIndex[V]) Put(term string, values []V) error {

	termValuesOffset := i.filePos
	index := make([]segmentIndexEntry[V], len(values)/i.segmentSize+1)

	// write segments
	for k := 0; k < len(values); k += i.segmentSize {

		segment := values[k:min(len(values), k+i.segmentSize)]
		sbuf, err := i.serialize(segment) // todo: wasted allocation?
		if err != nil {
			return fmt.Errorf("serializing values segment failed: %w", err)
		}

		sl, err := i.file.Write(sbuf)
		if err != nil {
			return err
		}
		i.filePos += int64(sl)

		index = append(index, segmentIndexEntry[V]{
			Offset: i.filePos,
			Min:    segment[0],
		})
	}

	// write segments index
	indexBuf, err := i.encodeSegmentsIndex(index)
	if err != nil {
		return fmt.Errorf("encoding segments index failed: %w", err)
	}

	n, err := i.file.Write(indexBuf)
	if err != nil {
		return fmt.Errorf("failed writing values index: %w", err)
	}
	i.filePos += int64(n)

	binary.BigEndian.PutUint32(i.lbuf, uint32(n))
	n, err = i.file.Write(i.lbuf)
	if err != nil {
		return fmt.Errorf("failed writing values index size for: %w", err)
	}
	i.filePos += int64(n)

	return i.fstBuilder.Insert([]byte(term), uint64(termValuesOffset))
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
	if err != nil {
		return nil, fmt.Errorf("fst: %w", err)
	}

	var (
		offset, nextOffset uint64
		existingTerm       []byte
		segments           []segmentIndexEntry[V]
		indexLen, n        int
		closeFile          sync.Once
	)
	sizeLen := 4
	buf := make([]byte, 4096)
	iterators := make([]lezhnev74.Iterator[V], 0)

	for _, term := range terms {

		// figure out term values offsets
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

		// read segment index
		_, err = i.file.Seek(max(int64(nextOffset)-int64(len(buf)), int64(offset)), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("reading values index: %w", err)
		}
		n, err = i.file.Read(buf)
		if err != nil {
			return nil, fmt.Errorf("reading values index: %w", err)
		}

		indexLen = int(binary.BigEndian.Uint32(buf[n-sizeLen:]))
		if indexLen > n-sizeLen { // do we need to read more bytes?
			_, err = i.file.Seek(int64(nextOffset)-int64(sizeLen)-int64(indexLen), io.SeekStart)
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

		segments, err = i.decodeSegmentsIndex(buf)
		if err != nil {
			return nil, fmt.Errorf("decoding values index: %w", err)
		}

		// Make a term iterator that scans through segments sequentially
		// makeTermFetchFunc return a function that can be used in an iterator,
		// upon calling the func it will return slices of sorted term's values
		makeTermFetchFunc := func(term string) func() ([]V, error) {
			var segmentBuf []byte
			si := 0

			return func() ([]V, error) {
				var s *segmentIndexEntry[V]
				for ; si < len(segments); si++ {
					if segments[si].Min < minVal || segments[si].Min > maxVal {
						continue
					}
					s = &segments[si]
					break
				}
				if s == nil {
					return nil, lezhnev74.EmptyIterator
				}

				_, err = i.file.Seek(s.Offset, io.SeekStart)
				if err != nil {
					return nil, fmt.Errorf("read values segment failed: %w", err)
				}

				if int64(len(segmentBuf)) < s.Len {
					segmentBuf = make([]byte, s.Len)
				} else {
					segmentBuf = segmentBuf[:s.Len]
				}

				_, err = i.file.Read(segmentBuf)
				if err != nil {
					return nil, fmt.Errorf("read values segment failed: %w", err)
				}

				values, err := i.unserialize(segmentBuf)
				if err != nil {
					return nil, fmt.Errorf("values: decompress fail: %w", err)
				}
				return values, nil
			}
		}

		closeIterator := func() error {
			closeFile.Do(func() { i.file.Close() })
			return nil
		}
		iterators = append(
			iterators,
			lezhnev74.NewDynamicSliceIterator(makeTermFetchFunc(term), closeIterator),
		)
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
	if err != nil {
		return nil, err
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

func (i *InvertedIndex[V]) encodeSegmentsIndex(segmentsIndex []segmentIndexEntry[V]) ([]byte, error) {
	// todo: apply columnar compression here
	sbuf := new(bytes.Buffer)
	enc := gob.NewEncoder(sbuf)
	err := enc.Encode(segmentsIndex)
	return sbuf.Bytes(), err
}
func (i *InvertedIndex[V]) decodeSegmentsIndex(data []byte) (index []segmentIndexEntry[V], err error) {
	sbuf := bytes.NewBuffer(data)
	enc := gob.NewDecoder(sbuf)
	err = enc.Decode(&index)
	return
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

func (i *InvertedIndex[V]) readFst() error {
	fstat, err := i.file.Stat()
	if err != nil {
		return err
	}

	buf := make([]byte, 4096)
	fstSizeLen := 4 // uint32 size

	minRead := min(fstat.Size(), int64(len(buf)))
	_, err = i.file.Seek(-minRead, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("fst: %w", err)
	}

	n, err := i.file.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("fst: %w", err)
	}

	if n < fstSizeLen {
		return fmt.Errorf("fst: no length bytes found, probably corrupted")
	}

	fstLen := int(binary.BigEndian.Uint32(buf[n-fstSizeLen:])) // read the fst len from the end of the stream
	if fstat.Size() < int64(fstLen+fstSizeLen) {
		return fmt.Errorf("fst: unexpected file size, probably corrupted")
	}

	if fstLen > n-fstSizeLen { // do we need to read more bytes?
		_, err := i.file.Seek(-int64(fstLen+fstSizeLen), io.SeekEnd)
		if err != nil {
			return err
		}

		buf = make([]byte, fstLen)
		_, err = i.file.Read(buf)
		if err != nil {
			return err
		}
	} else {
		buf = buf[:n-fstSizeLen]
	}

	i.fst, err = vellum.Load(buf[len(buf)-fstLen:]) // todo: use file for mmap io
	if err != nil {
		return fmt.Errorf("fst: load failed: %w", err)
	}
	return nil
}

func NewInvertedIndexUnit[V constraints.Ordered](filename string, segmentSize int) (InvertedIndexWriter[V], error) {
	var err error
	iiw := &InvertedIndex[V]{
		fstBuf:      new(bytes.Buffer),
		lbuf:        make([]byte, 4),
		serialize:   compressGob[V],
		unserialize: decompressGob[V],
		segmentSize: segmentSize,
	}

	iiw.file, err = os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	iiw.fstBuilder, err = vellum.New(iiw.fstBuf, nil)

	return iiw, err
}

func OpenInvertedIndex[V constraints.Ordered](file string) (InvertedIndexReader[V], error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	i := &InvertedIndex[V]{
		file:        f,
		fstBuf:      new(bytes.Buffer),
		lbuf:        make([]byte, 4),
		serialize:   compressGob[V],
		unserialize: decompressGob[V],
		cmp:         lezhnev74.OrderedCmpFunc[V],
	}

	err = i.readFooter()
	if err != nil {
		return nil, err
	}

	return i, nil
}
