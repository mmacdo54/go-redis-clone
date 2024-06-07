package main

import (
	"fmt"
	grc "github.com/mmacdo54/go-redis-clone/internal"
	"log"
	"net"
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

		go func(conn *net.Conn) {
			for {
				r := grc.NewReader(*conn)
				rv, err := r.Read()
				if err != nil {
					fmt.Println(err)
					(*conn).Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
					(*conn).Close()
					break
				} else {
					fmt.Println(rv)
					err := rv.HandleRespValue(conn)
					if err != nil {
						fmt.Println(err)
						(*conn).Write([]byte(fmt.Sprintf("-ERR %s\r\n", err)))
						(*conn).Close()
						break
					}
				}
			}
			(*conn).Close()
		}(&conn)
	}
}
