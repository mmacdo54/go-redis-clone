package handlers

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

type setValue struct {
	value  string
	expiry int
}

var SETs = map[string]setValue{}
var SETsMU = sync.RWMutex{}
var connections = map[string][]*net.Conn{}
var connectionMutex = sync.RWMutex{}

type handlerArgs struct {
	args []resp.RespValue
	conn *net.Conn
}
type Handler func(handlerArgs) resp.RespValue

var Handlers = map[string]Handler{
	"EXISTS":      exists,
	"SET":         set,
	"GET":         get,
	"DEL":         del,
	"PERSIST":     persist,
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

func exists(h handlerArgs) resp.RespValue {
	if len(h.args) == 0 {
		return resp.RespValue{Type: "error", Str: "ERR no keys passed to 'exists' command"}
	}

	count := 0
	for _, k := range h.args {
		SETsMU.RLock()
		if _, ok := SETs[k.Bulk]; ok {
			count++
		}
		SETsMU.RUnlock()
	}

	return resp.RespValue{Type: "integer", Num: count}
}

func set(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk
	var opts setOptions
	if len(h.args) > 2 {
		o, err := parseSetOptions(h.args[2:])
		if err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err.Error())}
		}
		opts = o
	}

	SETsMU.Lock()
	v, exists := SETs[key]

	if opts.nx && exists {
		SETsMU.Unlock()
		return resp.RespValue{Type: "null"}
	}

	if opts.xx && !exists {
		return resp.RespValue{Type: "null"}
	}

	s := setValue{value: value}

	if opts.keepttl && exists {
		s.expiry = v.expiry
	} else {
		switch {
		case opts.ex > 0:
			s.expiry = opts.ex
		case opts.px > 0:
			s.expiry = opts.px
		case opts.exat > 0:
			s.expiry = opts.exat
		case opts.pxat > 0:
			s.expiry = opts.pxat
		}
	}

	SETs[key] = s
	SETsMU.Unlock()

	if opts.get && !exists {
		return resp.RespValue{Type: "null"}
	}
	if opts.get {
		return resp.RespValue{Type: "bulk", Bulk: v.value}
	}
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

	now := int(time.Now().Unix()) * 1000

	if value.expiry > 0 && value.expiry < now {
		SETsMU.Lock()
		delete(SETs, key)
		SETsMU.Unlock()
		return resp.RespValue{Type: "null"}
	}

	return resp.RespValue{Type: "bulk", Bulk: value.value}
}

func del(h handlerArgs) resp.RespValue {
	// TODO allow multiple keys
	if len(h.args) == 0 {
		return resp.RespValue{Type: "error", Str: "ERR no keys passed to 'del' command"}
	}

	count := 0
	for _, k := range h.args {
		SETsMU.Lock()
		if _, ok := SETs[k.Bulk]; ok {
			count++
			delete(SETs, k.Bulk)
		}
		SETsMU.Unlock()
	}

	return resp.RespValue{Type: "integer", Num: count}
}

func persist(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of args passed to 'persist' command"}
	}

	key := h.args[0].Bulk
	SETsMU.RLock()
	s, ok := SETs[key]
	if !ok || s.expiry == 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}
	SETsMU.RUnlock()

	SETsMU.Lock()
	s.expiry = 0
	SETs[key] = s
	SETsMU.Unlock()

	return resp.RespValue{Type: "integer", Num: 1}
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
