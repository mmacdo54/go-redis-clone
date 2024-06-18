package handlers

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

const (
	STRING = "string"
	LIST   = "list"
)

type handlerArgs struct {
	args    []resp.RespValue
	conn    *net.Conn
	command string
	store   storage.Store
}
type Handler func(handlerArgs) resp.RespValue

var Handlers = map[string]Handler{
	"EXISTS":      exists,
	"SET":         set,
	"GET":         get,
	"DEL":         del,
	"COPY":        copy,
	"LPUSH":       lpush,
	"LPUSHX":      lpush,
	"LPOP":        lpop,
	"RPUSH":       rpush,
	"RPUSHX":      rpush,
	"RPOP":        rpop,
	"PERSIST":     persist,
	"EXPIRE":      setExpiry,
	"EXPIREAT":    setExpiry,
	"PEXPIRE":     setExpiry,
	"PEXPIREAT":   setExpiry,
	"EXPIRETIME":  expiretime,
	"SUBSCRIBE":   subscribe,
	"PUBLISH":     publish,
	"UNSUBSCRIBE": unsubscribe,
}

func HandleRespValue(v resp.RespValue, conn *net.Conn, store storage.Store) (resp.RespValue, error) {
	if v.Type != "array" {
		return resp.RespValue{}, errors.New("Only accept array type")
	}

	command := strings.ToUpper(v.Array[0].Bulk)
	args := v.Array[1:]
	handler, ok := Handlers[command]

	if !ok {
		return resp.RespValue{}, fmt.Errorf("Invalid command: %s", command)
	}

	return handler(handlerArgs{args: args, conn: conn, command: command, store: store}), nil
}
