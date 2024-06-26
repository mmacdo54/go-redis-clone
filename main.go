package main

import (
	"fmt"
	"io"
	"net"

	"github.com/mmacdo54/go-redis-clone/internal/configuration"
	"github.com/mmacdo54/go-redis-clone/internal/connection"
	"github.com/mmacdo54/go-redis-clone/internal/handlers"
	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func main() {
	store, err := storage.InitStore()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	config, err := configuration.InitConfig()
	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	fmt.Println("Listening for tcp connections on port 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}

		go handleConnection(conn, store, config)
	}
}

func handleConnection(conn net.Conn, store storage.Store, config configuration.Config) {
	defer conn.Close()
	c := connection.NewConnection(&conn)
	if !config.Requirepass {
		c.Validated = true
	}

	for {
		reader := resp.NewRespReader(conn)
		writer := resp.NewRespWriter(conn)
		val, err := reader.ReadResp()

		if err != nil {
			if err == io.EOF {
				fmt.Println("Client disconnected")
				break
			}
			if err := writer.WriteErrorResp(err); err != nil {
				fmt.Println(err)
			}
		}

		response := handlers.HandleRespValue(val, &c, store, config)

		if response.Type != resp.TYPE_VOID {
			writer.WriteResp(response)
		}
	}
}
