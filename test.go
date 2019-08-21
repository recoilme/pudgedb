package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/textproto"
)

func main() {
	listener, err := net.Listen("tcp", ":11211")

	if err != nil {
		fmt.Println(err)
		return
	}
	defer listener.Close()
	fmt.Println("Server is listening...")
	for {
		conn, err := listener.Accept()
		if err != nil {
			//fmt.Println(err)
			conn.Close()
			continue
		}
		go handleConnection(conn) // запускаем горутину для обработки запроса
	}
}

// обработка подключения
func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	tp := textproto.NewReader(reader)
	for {
		// read one line (ended with \n or \r\n)
		line, err := tp.ReadLineBytes()
		if err != nil {
			if err.Error() != "EOF" {
				//read tcp 127.0.0.1:11211->127.0.0.1:51639: read: connection reset by peer
				//log.Println(err)
			}
			break
		}
		if bytes.Equal(line[:1], []byte("c")) {
			fmt.Println(line, "- break")
			break
		}
		if bytes.Equal(line[:1], []byte("s")) {
			_, err := tp.ReadLine()
			if err != nil {
				break
			}
			conn.Write([]byte("STORED\r\n"))
		}
		if bytes.Equal(line[:1], []byte("g")) {
			conn.Write([]byte("VALUE value 0 3\r\nwor\r\nEND\r\n"))
		}

	}

}
