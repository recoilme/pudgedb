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
	"engine"
)

var (
	listenaddr = flag.String("l", "", "Interface to listen on. Default to all addresses.")
	network    = flag.String("n", "tcp", "Network to listen on (tcp,tcp4,tcp6,unix). unix not tested! Default is tcp")
	port       = flag.Int("p", 11211, "TCP port number to listen on (default: 11211)")
	threads    = flag.Int("t", runtime.NumCPU(), fmt.Sprintf("number of threads to use (default: %d)", runtime.NumCPU()))
	engine       = flag.String("e", "pudge", "database engine name.")
	
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

	crlf                    = []byte("\r\n")
	space                   = []byte(" ")
	resultOK                = []byte("OK\r\n")
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
	ErrCacheMiss = errors.New("memcache: cache miss")

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

	ctr, err := engine.getEngineCtr(engine)
	if err != nil {
		return err
	}

	//dbpath := path.Join(dir, "bench_"+engine)
	db, err := ctr(dbpath)
	if err != nil {
		return err
	}

	address := fmt.Sprintf("%s:%d", *listenaddr, *port)

	listener, err := net.Listen(*network, address)
	if err != nil {
		log.Fatalf("failed to serve: %s", err.Error())
		return
	}
	defer listener.Close()
	fmt.Printf("Server is listening on %s %s", *network, address)
	for {

		conn, err := listener.Accept()

		if err != nil {
			fmt.Println(err)
			conn.Close()
			continue
		}
		go listen(conn)
	}
}

func listen(c net.Conn) {
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
				err = OnSet(key, b[:size], flags, exp, size, noreply, rw)
				if err != nil {
					log.Println(err)
				}

			case bytes.HasPrefix(line, cmdGet), bytes.HasPrefix(line, cmdGetB), bytes.HasPrefix(line, cmdGets), bytes.HasPrefix(line, cmdGetsB):
				if bytes.Count(line, space) == 1 {
				}
				//// len args always more zero
				//args := strings.Split(string(lineb), " ")
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

// OnSet - hook on set
func OnSet(key string, val []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (err error) {
	//log.Println("OnSet", key, string(val))
	_, err = rw.Write(resultStored)
	if err != nil {
		return
	}
	err = rw.Flush()
	return
}

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
