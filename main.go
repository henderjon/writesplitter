package writesplitter

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

// a custom error to signal that no file was closed
var (
	ErrNotAFile = errors.New("WriteSplitter: invalid memory address or nil pointer dereference")
	ErrNotADir  = errors.New("WriteSplitter: specified dir is not a dir")
)

// WriteSplitter represents a disk bound io.WriteCloser that splits the input
// across consecutively named files based on either the number of bytes or the
// number of lines. Splitting does not guarantee true byte/line split
// precision as it does not parse the incoming data. The decision to split is
// before the underlying write operation based on the previous invocation. In
// other words, if a []byte sent to `Write()` contains enough bytes or new
// lines ('\n') to exceed the given limit, a new file won't be generated until
// the *next* invocation of `Write()`. If both LineLimit and ByteLimit are set,
// preference is given to LineLimit. By default, no splitting occurs because
// both LineLimit and ByteLimit are zero (0).
type WriteSplitter struct {
	Limit    int            // how many write ops (typically one per line) before splitting the file
	Dir      string         // files are named: $prefix + $nano-precision-timestamp + '.log'
	Prefix   string         // files are named: $prefix + $nano-precision-timestamp + '.log'
	Bytes    bool           // split by bytes and not lines
	numBytes int            // internal byte count
	numLines int            // internal line count
	handle   *os.File       // embedded file
}

// LineSplitter returns a WriteSplitter set to split at the given number of lines
func LineSplitter(limit int, dir, prefix string) *WriteSplitter {
	return &WriteSplitter{
		Limit:  limit,
		Dir:    filepath.Clean(dir),
		Prefix: filepath.Clean(prefix),
	}
}

// ByteSplitter returns a WriteSplitter set to split at the given number of bytes
func ByteSplitter(limit int, dir, prefix string) *WriteSplitter {
	return &WriteSplitter{
		Limit:  limit,
		Bytes:  true,
		Dir:    filepath.Clean(dir),
		Prefix: filepath.Clean(prefix),
	}
}

// Close is a passthru and satisfies io.Closer. Subsequent writes will return an
// error.
func (ws *WriteSplitter) Close() error {
	if ws.handle != nil { // do not try to close nil
		ws.numLines, ws.numBytes = 0, 0
		return ws.handle.Close()
	}
	return ErrNotAFile // do not hide errors, but signal it's a WriteSplit error as opposed to an underlying os.* error
}

// Write satisfies io.Writer and internally manages file io. Write also limits
// each WriteSplitter to only one open file at a time.
func (ws *WriteSplitter) Write(p []byte) (int, error) {

	var n int
	var e error

	if ws.handle == nil {
		e = ws.create()
	}

	switch {
	case ws.Limit > 0 && ws.Bytes && ws.numBytes >= ws.Limit:
		fallthrough
	case ws.Limit > 0 && ws.numLines >= ws.Limit:
		ws.Close()
		e = ws.create()
	}

	if e != nil {
		return 0, e
	}

	n, e = ws.handle.Write(p)
	ws.numLines += 1
	ws.numBytes += n
	return n, e
}

// CheckDir ensure that the given dir exists and is a dir
func CheckDir(dir string) error {
	dir = filepath.Clean(dir)
	stat, e := os.Stat(dir)
	if os.IsNotExist(e) || !stat.IsDir() || os.IsPermission(e) {
		return ErrNotADir
	}
	return nil
}

/// This is for mocking the file IO. Used exclusively for testing
///-----------------------------------------------------------------------------

// createFile is the file creating function that wraps os.Create
func (ws *WriteSplitter) create() error {

	if ws.Dir == "." { // avoid prefixing files with "."
		ws.Dir = ""
	}

	if ws.Prefix == "." { // avoid prefixing files with "."
		ws.Prefix = ""
	}

	filename := filepath.Join(ws.Dir, ws.Prefix+time.Now().Format(time.RFC3339Nano))

	f, e := os.Create(filename)
	if e == nil {
		ws.handle = f
	} else {
		ws.handle = nil
	}
	return e
}
