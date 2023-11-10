# Inverted Index

[![Go package](https://github.com/lezhnev74/inverted_index/actions/workflows/workflow.yml/badge.svg)](https://github.com/lezhnev74/inverted_index/actions/workflows/workflow.yml)

This repository contains code to manage inverted indexes. It introduces a file format to store and read them
efficiently.
The inverted index is a map of string terms to "postings" which are some ordered values (usually doc ids or timestamps).
The main use-case for this lib is to be used for storing inverted indexes of log files and that led to certain
compromises and design decisions. To name a few: the indexes are append only (no modifications are allowed, including
removal). Once the source log file is removed so is the index file. This supported the idea of keeping one index per
source log file. My [blog post](https://lessthan12ms.com/inverted-index.html) explains more ideas behind this.

## Installation And Usage

```
go get github.com/lezhnev74/inverted_index
```

### Working With Single Index File

Usage is simple, there is two phases: writing and reading. Also refer to tests files for more examples.

```go
// Phase-1: Writing To Index

// Since the index can work with different posting types, we have to provide two function for serialization and deserialization.
// The lib comes with an implementation for uint32 (used in case of timestamps as postings). Otherwise, write your own.
compressPostings := CompressUint32
decompressPostings := DecompressUint32
segmentSize := 1000 // internal size of postings segments, each segment can be read individually thus reducing I/O
indexWriter, err := NewInvertedIndexUnit(filename, segmentSize, compressPostings, decompressPostings)

// while you index your files, you put data to the index in order
// the writer accumulates data in memory
for term, postings := range indexingSource {
err = indexWriter.Put(term, postings)
}

// at this step the index file is finalized and closed
err = indexWriter.Close()

```

```go
// Phase-2: Reading Index

// Here we also need to provide the function to decompress posings from the file
indexReader, err := OpenInvertedIndex(filename, decompressPostings)

// We can read terms only from the index:
it, err := indexReader.ReadTerms()
allTerms := lezhnev74.ToSlice(it) // use as a slice or iterate individually

// We can read values for given terms and given constraints:
itValues, err := indexReader.ReadValues(allTerms[2:4], 0, math.MaxUint32) // or use ReadAllValues()

// use the iterators and close it
err = itValues.Close()
```

### Maintaining Multiple Index Files

During indexing of large source files we split the indexed data in small index files (to keep the memory usage low).
Eventually we want to read data from those as if it was a single index. `IndexDirectory` is a thread-safe service that
allows reading from multiple index files as if there was just one, it supports concurrent merging activity on the index
directory.

```go
var dirIndex *IndexDirectory[uint32] // set the type of the values in the index
var err error
dirIndex, err = NewIndexDirectory[uint32](
    path, // the directory where index files are located
    1000, // segments size (used for merging)
    single.CompressUint32, 
    single.DecompressUint32,
)

// Write to a new file:
w, err := dirIndex.NewWriter() // returns single.InvertedIndexWriter[T], read the docs about single index files API
// do the work and close:
err = w.Close()

// Read from multiple files:
r, err := indexDir.NewReader() // *IndexDirectoryReader[T]
termsIterator,err := r.ReadTerms()
// do the work and close:
err = termsIterator.Close()
```

#### Merging

Having many individual small files is beneficial during indexing, but these files can potentially contain duplicated data.
So merging is a process of collapsing multiple indexing files into a single file. 
This package supports merging and removing of merged files.

```go
var dirIndex *IndexDirectory[uint32]
dirIndex = NewIndexDirectory[uint32](...)

merger, err := dirIndex.NewMerger(2, 3) // min/max files to merge at a single pass
files, err := merger.Merge() // run one pass of merging (can be done in a loop in a separate goroutine)
err = merger.Cleanup() // remove merged files
```

## Design Details

To preserve disk space the index format applies a few compressing techniques:

- [FST](https://blog.burntsushi.net/transducers/) data structure to store terms
- all postings are stored together in segments, each segment is compressed with a provided user function (
  see `CompressUint32` as an example)
- to select term's postings it stores bitmaps for each term so selection works as application of a bitmap to all
  postings

These ideas come from the Lucene design. Compression here works only as good as the data that comes in. Highly random
data will be less compressible (applies to both terms and postings).

Here is the file format used for the index. Made with [asciiflow.com](https://asciiflow.com/).
```
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

```

## File Inspector

For a single index file we can run an inspection.
```go
// from source:
go run main.go ./testdata/newFile

Index File Summary
+----------------------------+--------------+
| File total size            |          413 |
| FST size                   |           75 |
| Terms count                |            6 |
| Terms min,max              | term0, term5 |
| Bitmaps size               |          122 |
| Values segments count/size |          8/2 |
| Values index size          |           64 |
| Values size                |          128 |
| Values count (approx.)     |        14-16 |
| Values min,max             |       1, 500 |
+----------------------------+--------------+
```

## Contribute

Open an issue and let's continue there.
