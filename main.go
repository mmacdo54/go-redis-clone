package main

import (
	"fmt"
	"log"
	"net"

	grc "github.com/mmacdo54/go-redis-clone/internal"
)

const (
	port = ":6379"
)

func main() {
	tcp, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Listening on %s\n", port)

	for {
		conn, err := tcp.Accept()

		if err != nil {
			log.Print(err)
			continue
		}

		go func(conn net.Conn) {
			r := grc.NewReader(conn)
			c, err := r.Read()
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
			} else {
				fmt.Println(c)
				conn.Write([]byte("+OK\r\n"))
			}
			conn.Close()
		}(conn)
	}
}
