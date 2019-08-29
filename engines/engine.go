package engine

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
)

// KvEngine implenets base key/value commands
type KvEngine interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (bool, error)
	Close() error
	FileSize() (int64, error)
}

type engineCtr func(string) (KvEngine, error)

var engines = map[string]engineCtr{
	//"pogreb":    newPogreb,
	//"goleveldb": newGolevelDB,
	//"bolt":      newBolt,
	//"badgerdb":  newBadgerdb,
	//"slowpoke":  newSlowpoke,
	"pudge": newPudge,
	//"buntdb":    newBunt,
}

// GetEngineCtr return engine by name
func GetEngineCtr(name string) (engineCtr, error) {
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
