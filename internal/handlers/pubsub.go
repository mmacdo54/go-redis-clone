package handlers

import (
	"fmt"
	"net"
	"slices"
	"sync"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

var connections = map[string][]*net.Conn{}
var connectionMutex = sync.RWMutex{}

func getAllChannels() (keys []string) {
	connectionMutex.RLock()
	for k := range connections {
		keys = append(keys, k)
	}
	connectionMutex.RUnlock()
	return
}

func isInChannel(conn *net.Conn, channel string) bool {
	connectionMutex.RLock()
	conns, ok := connections[channel]
	connectionMutex.RUnlock()

	if !ok {
		return false
	}

	return slices.ContainsFunc(conns, func(c *net.Conn) bool {
		return c == conn
	})
}

func sendMessageToConnection(conn *net.Conn, message resp.RespValue) {
	w := resp.NewRespWriter(*conn)
	w.WriteResp(message)
}

func sendMessageToChannel(channel string, message resp.RespValue) resp.RespValue {
	connectionMutex.RLock()
	connections := connections[channel]
	connectionMutex.RUnlock()

	var wg sync.WaitGroup
	wg.Add(len(connections))
	for _, c := range connections {
		go sendMessageToConnection(c, message)
		wg.Done()
	}
	wg.Wait()

	return generateIntegerResponse(len(connections))
}

func createSubMessage(channel string) resp.RespValue {
	return generateArrayResponse([]resp.RespValue{
		generateBulkResponse("subscribe"),
		generateBulkResponse(channel),
		generateIntegerResponse(1),
	})
}

func sendSubMessages(channels []string) {
	for _, c := range channels {
		message := createSubMessage(c)
		sendMessageToChannel(c, message)
	}
}

func createUnsubMessage(channel string) resp.RespValue {
	return generateArrayResponse([]resp.RespValue{
		generateBulkResponse("unsubscribe"),
		generateBulkResponse(channel),
		generateIntegerResponse(1),
	})
}

func sendUnsubMessages(channels []string) {
	for _, c := range channels {
		message := createUnsubMessage(c)
		sendMessageToChannel(c, message)
	}
}

func removeFromChannels(conn *net.Conn, channels []string) {
	unsubbedChannels := []string{}
	for _, c := range channels {
		if !isInChannel(conn, c) {
			return
		}
		connectionMutex.Lock()
		updatedConnections := []*net.Conn{}
		for _, c := range connections[c] {
			if conn != c {
				updatedConnections = append(updatedConnections, c)
			}
		}
		connections[c] = updatedConnections
		connectionMutex.Unlock()
		unsubbedChannels = append(unsubbedChannels, c)
	}
}

func subscribe(h handlerArgs) handlerResponse {
	if len(h.args) == 0 {
		return handlerResponse{
			err: fmt.Errorf("'subscribe' command needs at least one channel"),
		}
	}

	channels := []string{}

	for _, c := range h.args {
		channels = append(channels, c.Bulk)
	}

	connectionMutex.Lock()
	for _, c := range channels {
		if v, ok := connections[c]; ok && slices.Contains(v, h.conn.Conn) {
			continue
		}
		connections[c] = append(connections[c], h.conn.Conn)
	}
	connectionMutex.Unlock()

	sendSubMessages(channels)

	return handlerResponse{
		resp: generateVoidResponse(),
	}
}

func unsubscribe(h handlerArgs) handlerResponse {
	unsubChannels := []string{}
	if len(h.args) == 0 {
		unsubChannels = append(unsubChannels, getAllChannels()...)
	} else {
		for _, c := range h.args {
			unsubChannels = append(unsubChannels, c.Bulk)
		}
	}

	removeFromChannels(h.conn.Conn, unsubChannels)

	return handlerResponse{
		resp: generateStringResponse("OK"),
	}
}

func publish(h handlerArgs) handlerResponse {
	if len(h.args) != 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of arguments for 'publish' command"),
		}
	}

	channel := h.args[0].Bulk
	message := h.args[1].Bulk
	subMessage := generateArrayResponse([]resp.RespValue{
		generateBulkResponse("message"),
		generateBulkResponse(channel),
		generateBulkResponse(message),
	})

	return handlerResponse{
		resp: sendMessageToChannel(channel, subMessage),
	}
}
