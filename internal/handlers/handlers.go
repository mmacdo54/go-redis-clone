package handlers

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

var SETs = map[string]string{}
var SETsMU = sync.RWMutex{}
var HSETs = map[string]map[string]string{}
var HSETsMU = sync.RWMutex{}
var connections = map[string][]*net.Conn{}
var connectionMutex = sync.RWMutex{}

type handlerArgs struct {
	args []resp.RespValue
	conn *net.Conn
}
type Handler func(handlerArgs) resp.RespValue

var Handlers = map[string]Handler{
	"SET":         set,
	"GET":         get,
	"HSET":        hset,
	"HGET":        hget,
	"HGETALL":     hgetAll,
	"SUBSCRIBE":   subscribe,
	"PUBLISH":     publish,
	"UNSUBSCRIBE": unsubscribe,
}

func HandleRespValue(v resp.RespValue, conn *net.Conn) (resp.RespValue, error) {
	if v.Type != "array" {
		return resp.RespValue{}, errors.New("Only accept array type")
	}

	command := strings.ToUpper(v.Array[0].Bulk)
	args := v.Array[1:]
	handler, ok := Handlers[command]

	if !ok {
		return resp.RespValue{}, fmt.Errorf("Invalid command: %s", command)
	}

	return handler(handlerArgs{args, conn}), nil
}

func set(h handlerArgs) resp.RespValue {
	if len(h.args) != 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk

	SETsMU.Lock()
	SETs[key] = value
	SETsMU.Unlock()

	return resp.RespValue{Type: "string", Str: "OK"}
}

func get(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'get' command"}
	}

	key := h.args[0].Bulk

	SETsMU.RLock()
	value, ok := SETs[key]
	SETsMU.RUnlock()

	if !ok {
		return resp.RespValue{Type: "null"}
	}

	return resp.RespValue{Type: "bulk", Bulk: value}
}

func hset(h handlerArgs) resp.RespValue {
	if len(h.args) != 3 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'hset' command"}
	}
	hash := h.args[0].Bulk
	key := h.args[1].Bulk
	value := h.args[2].Bulk

	HSETsMU.Lock()
	if _, ok := HSETs[hash]; !ok {
		HSETs[hash] = map[string]string{}
	}
	HSETs[hash][key] = value
	HSETsMU.Unlock()

	return resp.RespValue{Type: "string", Str: "OK"}
}

func hget(h handlerArgs) resp.RespValue {
	if len(h.args) != 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'hget' command"}
	}

	hash := h.args[0].Bulk
	key := h.args[1].Bulk

	HSETsMU.RLock()
	val, ok := HSETs[hash][key]
	HSETsMU.RUnlock()

	if !ok {
		return resp.RespValue{Type: "null"}
	}

	return resp.RespValue{Type: "bulk", Bulk: val}
}

func hgetAll(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'hgetall' command"}
	}

	hash := h.args[0].Bulk

	HSETsMU.RLock()
	val, ok := HSETs[hash]
	HSETsMU.RUnlock()

	if !ok {
		return resp.RespValue{Type: "null"}
	}

	r := resp.RespValue{Type: "array", Array: []resp.RespValue{}}

	for _, v := range val {
		r.Array = append(r.Array, resp.RespValue{Type: "bulk", Bulk: v})
	}

	return r
}

func subscribe(h handlerArgs) resp.RespValue {
	// TODO HANDLE MULTIPLE CHANNEL ARGUMENTS
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
	connections[channel] = slices.DeleteFunc(connections[channel], func(c *net.Conn) bool {
		return c == h.conn
	})
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
