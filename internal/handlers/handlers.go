package handlers

import (
	"fmt"
	"net"
	"strings"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

const (
	STRING  = "string"
	LIST    = "list"
	SET     = "set"
	INTEGER = "integer"
)

type handlerArgs struct {
	args    []resp.RespValue
	conn    *net.Conn
	command string
	store   storage.Store
}
type handlerResponse struct {
	err  error
	resp resp.RespValue
}
type Handler func(handlerArgs) handlerResponse

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
	"LLEN":        llen,
	"LINDEX":      lindex,
	"SADD":        sadd,
	"SMEMBERS":    smembers,
	"SISMEMBER":   sismember,
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

func generateVoidResponse() resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_VOID}
}

func generateArrayResponse(arr []resp.RespValue) resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_ARRAY, Array: arr}
}

func generateSetResponse(set []resp.RespValue) resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_SET, Array: set}
}

func generateBulkResponse(bulk string) resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_BULK, Bulk: bulk}
}

func generateNullResponse() resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_NULL}
}

func generateStringResponse(str string) resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_STRING, Str: str}
}

func generateIntegerResponse(num int) resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_INTEGER, Num: num}
}

func generateErrorResponse(err error) resp.RespValue {
	return resp.RespValue{Type: resp.TYPE_ERROR, Str: fmt.Sprintf("ERR %s", err.Error())}
}

func HandleRespValue(v resp.RespValue, conn *net.Conn, store storage.Store) resp.RespValue {
	if v.Type != "array" {
		return generateErrorResponse(fmt.Errorf("Only accept array type"))
	}

	command := strings.ToUpper(v.Array[0].Bulk)
	args := v.Array[1:]
	handler, ok := Handlers[command]

	if !ok {
		return generateErrorResponse(fmt.Errorf("Invalid command: %s", command))
	}

	r := handler(handlerArgs{args: args, conn: conn, command: command, store: store})

	if r.err != nil {
		return generateErrorResponse(r.err)
	}

	return r.resp
}
