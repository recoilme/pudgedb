package engine

import (
	"bufio"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/dchest/siphash"
	_ "github.com/lib/pq"
)

var errNotImpl = errors.New("not implemented")

func newBloompg(params string, dbg bool) (KvEngine, error) {
	m, err := url.ParseQuery(params)
	if err != nil {
		log.Fatal(err)
	}
	connStr := "postgres://login:pass@127.0.0.1/database?sslmode=disable&connect_timeout=10"
	if len(m["connStr"]) > 0 {
		connStr = m["connStr"][0]
	} else {
		fmt.Println("connStr not set, fallback to default")
	}

	pg, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}

	bl := make(map[int]*bloom, 0)
	en, err := &bloompgEngine{Pg: pg, Bls: bl, Dbg: dbg}, err
	name, err := en.getSegmentName(9)
	fmt.Println(name, err)
	return en, err
}

type bloompgEngine struct {
	Pg  *sql.DB
	Bls map[int]*bloom
	Dbg bool
}

type bloom struct {
	bitmap []byte // bitmask
	k      int    // number of hashfunc
	power  int    // power of 2 (size of bitset)
}

func (en *bloompgEngine) Get(key []byte, rw *bufio.ReadWriter) ([]byte, bool, error) {
	var b []byte
	err := errNotImpl
	return b, false, err
}

// request - check filters:11,23 checks:uxrR45A8,xxxxyyyy
// response - xxxxyyyy: uxrR45A8:11,23
func (en *bloompgEngine) Gets(keys [][]byte, rw *bufio.ReadWriter) error {
	if len(keys) == 3 {
		ss := make([]string, 0)
		for _, key := range keys {
			ss = append(ss, string(key))
		}
		joined := strings.Join(ss, " ")
		//fmt.Println(joined)

		// scan segments
		segments := make([]int, 0)
		filters, checks, err := scanCheckLine(joined)
		if err == nil {
			args := strings.Split(filters, ",")
			for _, filter := range args {
				segment, err := strconv.Atoi(filter)
				if err == nil && segment > 0 {
					//log.Println(segment)
					if _, ok := en.Bls[segment]; !ok {
						if en.Dbg {
							fmt.Println("Loading segment", segment)
						}
						err := en.loadSegment(segment)
						if err != nil {
							fmt.Println(err)
						}
					} else {
						//loaded segment
						segments = append(segments, segment)
					}
				}
			}

			argschecks := strings.Split(checks, ",")
			res := ""
			for _, check := range argschecks {
				if en.Dbg {
					fmt.Println(check)
				}
				res += check + ":"
				for _, segm := range segments {
					if bl, ok := en.Bls[segm]; ok {
						// test bloom
						isin := true
						// take 2 siphash
						l1 := siphash.Hash(0, 0, []byte(check))
						l2 := siphash.Hash(1, 0, []byte(check))
						shift := 64 - bl.power
						for i := 0; i < bl.k; i++ {
							// get some int from hash by shift
							hashID := (l1 + uint64(i)*l2) >> uint(shift)

							if !bittest(bl.bitmap, int(hashID)) {
								isin = false
								break
							}
						}
						// is in bloom?
						if isin {
							if strings.HasSuffix(res, ":") {
								res += fmt.Sprintf("%d", segm)
							} else {
								res += fmt.Sprintf(",%d", segm)
							}
						}
					}
				}
				res += " "
			}
			fmt.Fprintf(rw, "VALUE check 0 %d\r\n%s\r\n", len([]byte(res)), res)
		} else {
			fmt.Println(err)
		}
	} else {
		fmt.Println("wrong params len, need 3, got:", len(keys))
	}

	_, err := rw.Write([]byte("END\r\n"))
	if err == nil {
		err = rw.Flush()
	}
	return err
}

//BIT_TEST(bitfield, bit) ((bitfield)[(uint64_t)(bit) / 8] & (1 << ((uint64_t)(bit) % 8)))
func bittest(bitfield []byte, bit int) bool {
	if bit/8 >= len(bitfield) {
		//log.Println("out of range",bit/8,len(bitfield))
		return true
	}
	bb := bitfield[bit/8] & (1 << (uint(bit) % 8))
	//log.Println("bb",bb)
	return bb != 0
}

func (en *bloompgEngine) Close() error {
	return en.Pg.Close()
}

func (en *bloompgEngine) FileSize() (int64, error) {
	return int64(0), nil
}

func (en *bloompgEngine) Set(key, value []byte, flags uint32, exp int32, size int, noreply bool, rw *bufio.ReadWriter) (bool, error) {
	return false, errNotImpl
}

func (en *bloompgEngine) Delete(key []byte, rw *bufio.ReadWriter) (isFound bool, noreply bool, err error) {
	return false, false, errNotImpl
}

func (en *bloompgEngine) Incr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error) {
	return 0, false, false, errNotImpl

}

func (en *bloompgEngine) Decr(key []byte, value uint64, rw *bufio.ReadWriter) (result uint64, isFound bool, noreply bool, err error) {
	return 0, false, false, errNotImpl
}

func (en *bloompgEngine) getSegmentName(id int) (name string, err error) {
	rows, err := en.Pg.Query("SELECT name FROM adroom_outer_cookie_segments WHERE id = $1", id)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&name)
		if err != nil {
			return
		}
		break
	}
	return
}

func (en *bloompgEngine) loadSegment(id int) (err error) {
	rows, err := en.Pg.Query("SELECT bloomfilter_raw FROM adroom_outer_cookie_segments WHERE id = $1", id)
	if err != nil {
		fmt.Println("SELECT err", err, id)
		return
	}
	defer rows.Close()
	var b []byte
	for rows.Next() {
		//log.Println("start scan")
		err = rows.Scan(&b)
		if err != nil {
			fmt.Println("SELECT scan err", err, id)
			return
		}
		break
	}
	if len(b) < 2 {
		return fmt.Errorf("zero len, id:%d", id)
	}

	/*
		// test buffer, with Wikipedia and 123 in
		buf := []byte{4, 5, 4, 194, 67}
		b = buf
	*/

	//parse k
	bs := make([]byte, 8)
	bs[0] = b[0]
	k := binary.LittleEndian.Uint64(bs)

	//parse power
	bs2 := make([]byte, 8)
	bs2[0] = b[1]
	power := binary.LittleEndian.Uint64(bs2)

	fmt.Printf("Loaded, id:%d k:%d power:%d len:%d", id, k, power, len(b))

	bl := &bloom{}
	bl.bitmap = b[2:]
	bl.k = int(k)
	bl.power = int(power)
	en.Bls[id] = bl
	return
}

// scanCheckLine populates it and returns the declared params of the item.
// format: check filters:11,23 checks:uxrR45A8,xxxxyyyy
func scanCheckLine(line string) (filters, checks string, err error) {
	pattern := "check filters:%s checks:%s\r\n"
	dest := []interface{}{&filters, &checks}
	n, err := fmt.Sscanf(line, pattern, dest...)
	if n != len(dest) {
		err = errors.New("wrong pattern")
	}
	return
}
