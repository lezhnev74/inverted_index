package multiple

import (
	"golang.org/x/exp/slices"
	"sync"
)

type indexFile struct {
	// rlock is only used to track active readers (so the file can't be deleted)
	rlock sync.RWMutex
	/* Full path to the file */
	path string
	/* len is used in merging policy to merge the smallest files first */
	len int64
}

/*
filesList is a collection of individual immutable index files.
*/
type filesList struct {
	files []*indexFile
	// lock is used to protect append/delete operations.
	// rlock is used to allow concurrent reads.
	lock sync.RWMutex
}

func NewFilesList() *filesList {
	return &filesList{
		files: make([]*indexFile, 0),
		lock:  sync.RWMutex{},
	}
}

func (f *filesList) safeRead(fn func()) {
	f.lock.RLock()
	defer f.lock.RUnlock()

	fn()
}

func (f *filesList) safeWrite(fn func()) {
	f.lock.Lock()
	defer f.lock.Unlock()

	fn()
}

func (f *filesList) putFile(path string, fileSize int64) {

	newFile := &indexFile{
		path:  path,
		len:   fileSize,
		rlock: sync.RWMutex{},
	}

	// For the purposes of merging, files are sorted by size asc
	pos, _ := slices.BinarySearchFunc(f.files, newFile, func(a, b *indexFile) int {
		if a.len < b.len {
			return -1
		} else if a.len > b.len {
			return 1
		}
		return 0
	})

	f.files = append(
		f.files[:pos],
		append([]*indexFile{newFile}, f.files[pos:]...)...,
	)
}
