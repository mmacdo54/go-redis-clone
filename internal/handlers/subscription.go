package handlers

import (
	"net"
	"sync"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

var connections = map[string][]*net.Conn{}
var connectionMutex = sync.RWMutex{}

func subscribe(h handlerArgs) resp.RespValue {
	// TODO HANDLE MULTIPLE CHANNEL ARGUMENTS
	// TODO DEDUPE EXISTING CONNECTIONS
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'subscribe' command"}
	}

	channel := h.args[0].Bulk

	connectionMutex.Lock()
	connections[channel] = append(connections[channel], h.conn)
	connectionMutex.Unlock()

	return resp.RespValue{
		Type: "array",
		Array: []resp.RespValue{
			{Type: "bulk", Bulk: "subscribe"},
			{Type: "bulk", Bulk: channel},
			{Type: "integer", Num: 1},
		},
	}
}

func unsubscribe(h handlerArgs) resp.RespValue {
	// TODO HANDLE NO ARGUMENTS SHOULD UNSUBSCRIBE FROM ALL CHANNELS
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR 'unsubscribe' requires 1 argument"}
	}

	channel := h.args[0].Bulk
	connectionMutex.Lock()
	updatedConnections := []*net.Conn{}
	for _, conn := range connections[channel] {
		if h.conn != conn {
			updatedConnections = append(updatedConnections, conn)
		}
	}
	connections[channel] = updatedConnections
	connectionMutex.Unlock()

	return resp.RespValue{
		Type: "array",
		Array: []resp.RespValue{
			{Type: "bulk", Bulk: "unsubscribe"},
			{Type: "bulk", Bulk: "channel"},
			{Type: "integer", Num: 1},
		},
	}
}

func publish(h handlerArgs) resp.RespValue {
	if len(h.args) != 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'publish' command"}
	}

	channel := h.args[0].Bulk
	message := h.args[1].Bulk
	connectionMutex.RLock()
	cs := connections[channel]
	connectionMutex.RUnlock()

	subMessage := resp.RespValue{
		Type: "array",
		Array: []resp.RespValue{
			{Type: "bulk", Bulk: "message"},
			{Type: "bulk", Bulk: channel},
			{Type: "bulk", Bulk: message},
		},
	}
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c *net.Conn, wg *sync.WaitGroup) {
			w := resp.NewRespWriter(*c)
			w.WriteResp(subMessage)
			wg.Done()
		}(c, &wg)
	}
	wg.Wait()
	return resp.RespValue{Type: "integer", Num: len(cs)}
}
