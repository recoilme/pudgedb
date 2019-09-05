package engine

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
)

// KvEngine implenets base key/value commands
type KvEngine interface {
	Get(key []byte, rw *bufio.ReadWriter) (value []byte, noreply bool, err error)
	Gets(keys [][]byte, rw *bufio.ReadWriter) error
	Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (noreplyresp bool, err error)
	Incr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error)
	Decr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error)
	Delete(key []byte, rw *bufio.ReadWriter) (isFound bool, noreply bool, err error)
	Close() error
	FileSize() (int64, error)
}

// Ctr - engine controller
type Ctr func(string, bool) (KvEngine, error)

var engines = map[string]Ctr{
	//"pogreb":    newPogreb,
	//"goleveldb": newGolevelDB,
	//"bolt":      newBolt,
	//"badgerdb":  newBadgerdb,
	"pudge":   newPudge,
	"bloompg": newBloompg,
	//"buntdb":    newBunt,
}

// GetEngineCtr return engine by name
func GetEngineCtr(name string) (Ctr, error) {
	if ctr, ok := engines[name]; ok {
		return ctr, nil
	}
	return nil, errors.New("unknown engine")
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}
