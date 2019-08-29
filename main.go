package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"strings"

	engine "github.com/recoilme/pudgedb/engines"
)

var (
	listenaddr = flag.String("l", "", "Interface to listen on. Default to all addresses.")
	network    = flag.String("n", "tcp", "Network to listen on (tcp,tcp4,tcp6,unix). unix not tested! Default is tcp")
	port       = flag.Int("p", 11211, "TCP port number to listen on (default: 11211)")
	threads    = flag.Int("t", runtime.NumCPU(), fmt.Sprintf("number of threads to use (default: %d)", runtime.NumCPU()))
	enginename = flag.String("e", "pudge", "database engine name.")
)

var (
	cmdSet    = []byte("set")
	cmdSetB   = []byte("SET")
	cmdGet    = []byte("get")
	cmdGetB   = []byte("GET")
	cmdGets   = []byte("gets")
	cmdGetsB  = []byte("GETS")
	cmdClose  = []byte("close")
	cmdCloseB = []byte("CLOSE")

	crlf     = []byte("\r\n")
	space    = []byte(" ")
	resultOK = []byte("OK\r\n")

	resultStored            = []byte("STORED\r\n")
	resultNotStored         = []byte("NOT_STORED\r\n")
	resultExists            = []byte("EXISTS\r\n")
	resultNotFound          = []byte("NOT_FOUND\r\n")
	resultDeleted           = []byte("DELETED\r\n")
	resultEnd               = []byte("END\r\n")
	resultOk                = []byte("OK\r\n")
	resultError             = []byte("ERROR\r\n")
	resultTouched           = []byte("TOUCHED\r\n")
	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
)

// Similar to:
// https://godoc.org/google.golang.org/appengine/memcache

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss ")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServerError means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")
)

func main() {

	ctr, err := engine.GetEngineCtr(*enginename)
	if err != nil {
		log.Fatal(err)
	}
	db, err := ctr("db")
	if err != nil {
		log.Fatal(err)
	}

	//engine.GetEngineCtr("")
	//engine.GetEngineCtr("hashmap")
	address := fmt.Sprintf("%s:%d", *listenaddr, *port)

	listener, err := net.Listen(*network, address)
	if err != nil {
		log.Fatalf("failed to serve: %s", err.Error())
		return
	}
	defer listener.Close()
	fmt.Printf("\nServer is listening on %s %s\n", *network, address)
	for {

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println(err)
			conn.Close()
			continue
		}
		go listen(conn, db)
	}
}

// as described https://github.com/memcached/memcached/blob/master/doc/protocol.txt
func listen(c net.Conn, db engine.KvEngine) {
	defer c.Close()
	for {
		rw := bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
		line, err := rw.ReadSlice('\n')

		if err != nil {
			if err.Error() != "EOF" {
				//network error and so on
				//log.Println(err)
			} else {
				break //close connection
			}
		}
		if len(line) > 0 {
			switch {
			case bytes.HasPrefix(line, cmdSet), bytes.HasPrefix(line, cmdSetB):
				//log.Println("set", line)
				key, flags, exp, size, noreply, err := scanSetLine(line, bytes.HasPrefix(line, cmdSetB))
				if err != nil || size == -1 {
					log.Println(err, size)
					rw.Write(resultError)
					rw.Flush()
					err = nil
					break
				}
				b := make([]byte, size+2)
				_, err = io.ReadFull(rw, b)
				if err != nil {
					break
				}
				noreply, err = db.Set([]byte(key), b[:size], flags, exp, size, noreply, rw)
				if err != nil {
					log.Println(err)
				}
				if !noreply {
					if err != nil {
						_, err = rw.Write(resultNotStored)
						err = nil
					} else {
						_, err = rw.Write(resultStored)
					}
					if err != nil {
						break
					}
					err = rw.Flush()
					if err != nil {
						break
					}
				}

			case bytes.HasPrefix(line, cmdGet), bytes.HasPrefix(line, cmdGetB), bytes.HasPrefix(line, cmdGets), bytes.HasPrefix(line, cmdGetsB):
				cntspace := bytes.Count(line, space)
				if cntspace == 0 || !bytes.HasSuffix(line, crlf) {
					err = protocolError(rw)
					if err != nil {
						break
					}
				}

				if cntspace == 1 {
					key := line[(bytes.Index(line, space) + 1) : len(line)-2]
					//log.Println("'" + string(key) + "'")
					value, err := db.Get(key)
					if err == nil && value != nil {
						fmt.Fprintf(rw, "VALUE %s 0 %d\r\n%s\r\n", key, len(value), value)
					}
					rw.Write(resultEnd)
					rw.Flush()
				}
				// len args always more zero
				args := strings.Split(string(line), " ")
				gets(args[1:])
				//fmt.Fprintf(rw, "VALUE a 0 3\r\n123\r\nEND\r\n")
				//rw.Flush()
				//log.Println("get", line)
				//for _, arg := range args[1:] {
			case bytes.HasPrefix(line, cmdClose), bytes.HasPrefix(line, cmdCloseB):
				err = errors.New("Close")
				break
			}

			//check err
			if err != nil {
				if resumableError(err) {
					log.Println(err)
				} else {
					break //close connection
				}
			}

		}
	}

}
func gets(s []string) {}

// scanSetLine populates it and returns the declared size of the item.
// It does not read the bytes of the item.
func scanSetLine(line []byte, isCap bool) (key string, flags uint32, exp int32, size int, noreply bool, err error) {
	//set := ""
	noreplys := ""
	noreply = false
	cmd := "set"
	if isCap {
		cmd = "SET"
	}
	pattern := cmd + " %s %d %d %d %s\r\n"
	dest := []interface{}{&key, &flags, &exp, &size, &noreplys}
	if bytes.Count(line, space) == 4 {
		pattern = cmd + " %s %d %d %d\r\n"
		dest = dest[:4]
	}
	if noreplys == "noreply" || noreplys == "NOREPLY" {
		noreply = true
	}
	n, err := fmt.Sscanf(string(line), pattern, dest...)
	if n != len(dest) {
		size = -1
	}
	return
}

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

func isASCIILetter(b byte) bool {
	b |= 0x20 // make lower case
	return 'a' <= b && b <= 'z'
}

func protocolError(rw *bufio.ReadWriter) (err error) {
	_, err = rw.Write(resultError)
	if err != nil {
		return
	}
	err = rw.Flush()
	return
}
