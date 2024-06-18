package handlers

import (
	"fmt"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func lpush(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'lpush' command"}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
	}

	if h.command == "LPUSHX" && !ok {
		return resp.RespValue{Type: "error", Str: "ERR key does not exist"}
	}

	if ok && el.Typ != "" && el.Typ != LIST {
		return resp.RespValue{Type: "error", Str: "ERR value stored at key is not a list"}
	}

	el.Key = key
	el.Typ = LIST
	now := int(time.Now().Unix()) * 1000
	if el.Exp > 0 && el.Exp < now {
		el.Arr = []string{}
		el.Exp = 0
	}

	list := []string{}
	for i := len(h.args) - 1; i >= 1; i-- {
		list = append(list, h.args[i].Bulk)
	}

	if ok {
		list = append(list, el.Arr...)
	}
	el.Arr = list
	err = h.store.SetKV(el)

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
	}

	return resp.RespValue{Type: "integer", Num: len(list)}
}

func rpush(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'rpush' command"}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
	}

	if h.command == "RPUSHX" && !ok {
		return resp.RespValue{Type: "error", Str: "ERR key does not exist"}
	}

	if ok && el.Typ != "" && el.Typ != LIST {
		return resp.RespValue{Type: "error", Str: "ERR value stored at key is not a list"}
	}

	el.Key = key
	el.Typ = LIST
	now := int(time.Now().Unix()) * 1000
	if el.Exp > 0 && el.Exp < now {
		el.Arr = []string{}
		el.Exp = 0
	}

	for _, v := range h.args[1:] {
		el.Arr = append(el.Arr, v.Bulk)
	}

	err = h.store.SetKV(el)

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
	}

	return resp.RespValue{Type: "integer", Num: len(el.Arr)}
}

func lpop(h handlerArgs) resp.RespValue {
	// TODO handle a range
	if len(h.args) == 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'lpop' command"}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
	}

	if !ok || el.Typ != LIST || len(el.Arr) == 0 {
		return resp.RespValue{Type: "null"}
	}

	now := int(time.Now().Unix()) * 1000
	if el.Exp > 0 && el.Exp < now {
		if _, err := h.store.DeleteByKey(el); err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}
		return resp.RespValue{Type: "null"}
	}

	val := el.Arr[0]
	if len(el.Arr) == 1 {
		if _, err := h.store.DeleteByKey(el); err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}
	} else {
		el.Arr = el.Arr[1:]
		if err := h.store.SetKV(el); err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}
	}

	return resp.RespValue{Type: "bulk", Bulk: val}
}

func rpop(h handlerArgs) resp.RespValue {
	// TODO handle a range
	if len(h.args) == 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'rpop' command"}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
	}

	if !ok || el.Typ != LIST || len(el.Arr) == 0 {
		return resp.RespValue{Type: "null"}
	}

	now := int(time.Now().Unix()) * 1000
	if el.Exp > 0 && el.Exp < now {
		if _, err := h.store.DeleteByKey(el); err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}
		return resp.RespValue{Type: "null"}
	}

	val := el.Arr[len(el.Arr)-1]
	if len(el.Arr) == 1 {
		if _, err := h.store.DeleteByKey(el); err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}
	} else {
		el.Arr = el.Arr[:len(el.Arr)-1]
		if err := h.store.SetKV(el); err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}
	}

	return resp.RespValue{Type: "bulk", Bulk: val}
}
