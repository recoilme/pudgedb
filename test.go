package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
)

var (
	mu       = &sync.RWMutex{}
	kv       = make(map[string][]byte)
	errProto = errors.New("ERROR")
)

func main() {
	listener, err := net.Listen("tcp", ":11211")

	if err != nil {
		log.Fatalf("failed to serve: %s", err.Error())
		return
	}
	defer listener.Close()
	fmt.Println("Server is listening...")
	for {

		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			conn.Close()
			continue
		}
		go handleConnection(textproto.NewConn(conn)) // запускаем горутину для обработки сокета
	}
}

// обработка мемкеш протокола по заветам Ильича - Фитцпатрика
// https://github.com/memcached/memcached/blob/master/doc/protocol.txt
func handleConnection(c *textproto.Conn) {
	defer c.Close()

	for {
		var err error
		// read one line (ended with \n or \r\n)
		line, err := c.ReadLine()
		// check protocol error
		if err != nil {
			if err.Error() != "EOF" {
				//network error and so on
			}
			break
		}
		if line == "" {
			// strange
			continue
		}
		// len args always more zero
		args := strings.Split(line, " ")

		// для кривых клиентов
		cmd := strings.ToLower(args[0])
		if cmd == "gets" {
			cmd = "get" // similar
		}
		switch cmd {
		case "set":
			// norepy is optional, but always last param
			noreply := false
			if args[len(args)-1] == "noreply" {
				noreply = true
			}
			if len(args) < 5 || args[1] == "" {
				// вот тут я странно пукнул насчет ключа пустого, но еще странней обнаружить там значение однажды
				err = errProto
				break
			}
			bytes, err := strconv.Atoi(args[4])
			if err != nil || bytes == 0 {
				//log.Println(bytes, err)
				err = errProto
				break
			}
			b := make([]byte, bytes)
			// тут читаем напрямик, так и быстрей и безопасно для байтов
			n, err := c.R.Read(b)

			if err != nil || n != bytes {
				//log.Println(n != bytes, err)
				err = errProto
				break
			}
			// не забудем про /r/n
			crlf := make([]byte, 2)
			_, err = c.R.Read(crlf)

			mu.Lock()
			kv[args[1]] = b
			mu.Unlock()
			if !noreply {
				err = c.PrintfLine("STORED")
			}

		case "get":
			if len(args) < 2 {
				err = errProto
				break
			}
			for _, arg := range args[1:] {
				mu.RLock()
				b, ok := kv[arg]
				mu.RUnlock()
				if ok {
					// If some of the keys appearing in a retrieval request are not sent back
					// by the server in the item list this means that the server does not
					// hold items with such keys

					fmt.Fprintf(c.W, "VALUE %s 0 %d\r\n%s\r\n", arg, len(b), b)
					//err = c.PrintfLine("VALUE %s 0 %d\r\n%s\r\nEND", args[1], len(b), b)
				}
			}
			fmt.Fprintf(c.W, "END\r\n")
			// отцы копят в буфере, потом зараз кидают в трубу, для быстродействия полезно это
			// стандартный протокол шлет построчно - медленней в 2 раза будет
			err = c.W.Flush()

		case "delete":
			if len(args) < 2 {
				err = errProto
				break
			}
			mu.RLock()
			_, ok := kv[args[1]]
			mu.RUnlock()
			if ok {
				mu.Lock()
				delete(kv, args[1])
				mu.Unlock()
				c.Writer.PrintfLine("DELETED")

			} else {
				c.Writer.PrintfLine("NOT_FOUND")
			}

		case "close":
			err = errors.New("CLOSE")

		default:
			if len([]byte(cmd)) > 0 && !isASCIILetter([]byte(cmd)[0]) {
				// This is SPARTA!
				err = errors.New("CLOSE")
			} else {
				err = errProto
			}
			log.Println("default", line, cmd, len(line))

		}
		// если у нас ошибка в цикле - это повод соскочить с сокета
		if err != nil {
			if err == errProto {
				if c.PrintfLine("ERROR") != nil {
					// не удалось записать в сокет
					break
				}
				err = nil
				// можно считать кол-во ошибок и спрыгивать - если там мусор
				//break
			} else {
				if err.Error() != "CLOSE" {
					fmt.Println(err)
				}
				//Ошибка на сервере
				break
			}
		}
	}
}

func isASCIILetter(b byte) bool {
	b |= 0x20 // make lower case
	return 'a' <= b && b <= 'z'
}
