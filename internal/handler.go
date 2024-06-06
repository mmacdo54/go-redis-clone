package grc

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

var connections = map[string][]*net.Conn{}
var connectionMutex = sync.RWMutex{}

type handler func([]RespValue, *net.Conn) error

var (
	handlers = map[string]handler{"SUBSCRIBE": subscribe, "PUBLISH": publish}
)

func (rv *RespValue) HandleRespValue(conn *net.Conn) error {
	if rv.Type != "array" {
		return fmt.Errorf("Only handle array types")
	}

	command := strings.ToUpper(rv.Arr[0].Bulk)
	args := rv.Arr[1:]
	handler, ok := handlers[command]

	if !ok {
		return fmt.Errorf("Do not handle %s command", command)
	}

	return handler(args, conn)
}

func subscribe(ra []RespValue, conn *net.Conn) error {
	if len(ra) != 1 {
		return fmt.Errorf("SUBSCRIBE is expecting one argument")
	}

	channel := ra[0].Bulk
	connectionMutex.Lock()
	connections[channel] = append(connections[channel], conn)
	connectionMutex.Unlock()
	return nil
}

func publish(ra []RespValue, conn *net.Conn) error {
	if len(ra) != 2 {
		return fmt.Errorf("PUBLISH is expecting two arguments")
	}
	channel := ra[0].Bulk
	message := ra[1].Bulk
	connectionMutex.RLock()
	cs := connections[channel]
	connectionMutex.RUnlock()

	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c *net.Conn, wg *sync.WaitGroup) {
			if c != conn {
				fmt.Println("Sending")
				(*c).Write([]byte(fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(message), message)))
			}
			wg.Done()
		}(c, &wg)
	}
	wg.Wait()
	(*conn).Write([]byte(fmt.Sprintf(":%d\r\n", len(cs))))
	return nil
}
