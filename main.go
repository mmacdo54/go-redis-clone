package main

import (
	"fmt"
	"net"
	"strings"

	"github.com/mmacdo54/go-redis-clone/internal/handlers"
	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func main() {
	l, err := net.Listen("tcp", ":6379")

	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Listening for tcp connections on port 6379")

	store, err := storage.NewStore("database.aof")

	if err != nil {
		fmt.Println(err)
		return
	}
	defer store.Close()
	store.Read(func(v resp.RespValue) {
		if _, err := handlers.HandleRespValue(v, nil); err != nil {
			fmt.Println(err)
			return
		}
	})

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(conn, store)
	}
}

func handleConnection(conn net.Conn, store *storage.Store) {
	defer conn.Close()
	for {
		reader := resp.NewRespReader(conn)
		writer := resp.NewRespWriter(conn)
		val, err := reader.ReadResp()

		if err != nil {
			if err := writer.WriteErrorResp(err); err != nil {
				fmt.Println(err)
			}
			continue
		}

		response, err := handlers.HandleRespValue(val, &conn)

		if err != nil {
			if err := writer.WriteErrorResp(err); err != nil {
				fmt.Println(err)
			}
			continue
		}

		command := strings.ToUpper(val.Array[0].Bulk)

		if command == "SET" || command == "HSET" {
			store.Write(val)
		}

		writer.WriteResp(response)
	}
}
