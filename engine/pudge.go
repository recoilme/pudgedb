package engine

import (
	"bufio"
	"fmt"
	"log"
	"net/url"
	"sync"

	"github.com/recoilme/pudge"
)

// newPudge open new pudge database
// params - path, example: -params path=db
// set db, if not set
func newPudge(params string, dbg bool) (KvEngine, error) {

	m, err := url.ParseQuery(params)
	if err != nil {
		log.Fatal(err)
	}
	path := "db"
	if len(m["path"]) > 0 {
		path = m["path"][0]
	} else {
		log.Println("path not set, fallback to db (-params path=db)")
	}

	cfg := pudge.DefaultConfig

	cfg.StoreMode = 2 //uncomment for inmemory mode
	db, err := pudge.Open(path, cfg)
	return &pudgeEngine{Db: db, Path: path}, err
}

type pudgeEngine struct {
	Db   *pudge.Db
	Path string
}

func (en *pudgeEngine) Get(key []byte, rw *bufio.ReadWriter) ([]byte, bool, error) {
	var b []byte
	err := en.Db.Get(key, &b)
	return b, false, err
}

func (en *pudgeEngine) Gets(keys [][]byte, rw *bufio.ReadWriter) error {
	var wg sync.WaitGroup
	read := func(key []byte) {
		defer wg.Done()
		var b []byte
		err := en.Db.Get(key, &b)
		if err == nil {
			fmt.Fprintf(rw, "VALUE %s 0 %d\r\n%s\r\n", key, len(b), b)
		}
	}
	wg.Add(len(keys))
	for _, key := range keys {
		//log.Println(string(key))
		go read(key)
	}
	wg.Wait()
	_, err := rw.Write([]byte("END\r\n"))
	if err == nil {
		err = rw.Flush()
	}
	return err
}

func (en *pudgeEngine) Close() error {
	return en.Db.Close()
}

func (en *pudgeEngine) FileSize() (int64, error) {
	return en.Db.FileSize()
}

func (en *pudgeEngine) Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (bool, error) {
	err := en.Db.Set(key, value)
	return false, err
}

func (en *pudgeEngine) Delete(key []byte, rw *bufio.ReadWriter) (isFound bool, noreply bool, err error) {
	err = en.Db.Delete(key)
	if err != nil {
		return false, false, err
	}
	return true, false, err
}

func (en *pudgeEngine) Incr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error) {
	res, err := en.Db.Counter(key, int(value))
	if err != nil {
		return 0, false, false, err
	}
	return uint64(res), true, false, err
}

func (en *pudgeEngine) Decr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error) {
	res, err := en.Db.Counter(key, (-1)*int(value))
	if err != nil {
		return 0, false, false, err
	}
	return uint64(res), true, false, err
}
