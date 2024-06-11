package handlers

import (
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

func lpush(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'lpush' command"}
	}

	key := h.args[0].Bulk
	setsMU.RLock()
	el, ok := sets[key]
	setsMU.RUnlock()

	now := int(time.Now().Unix()) * 1000
	if el.expiry > 0 && el.expiry < now {
		el.list = []string{}
		el.expiry = 0
	}

	if h.command == "LPUSHX" && !ok {
		return resp.RespValue{Type: "error", Str: "ERR key does not exist"}
	}

	if ok && el.typ != "" && el.typ != LIST {
		return resp.RespValue{Type: "error", Str: "ERR value stored at key is not a list"}
	}

	el.typ = LIST
	list := []string{}
	for i := len(h.args) - 1; i >= 1; i-- {
		list = append(list, h.args[i].Bulk)
	}

	setsMU.Lock()
	if ok {
		list = append(list, el.list...)
	}
	el.list = list
	sets[key] = el
	setsMU.Unlock()

	return resp.RespValue{Type: "integer", Num: len(list)}
}

func lpop(h handlerArgs) resp.RespValue {
	// TODO handle a range
	if len(h.args) == 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'lpop' command"}
	}

	key := h.args[0].Bulk

	setsMU.RLock()
	v, ok := sets[key]
	setsMU.RUnlock()

	if !ok || v.typ != LIST || len(v.list) == 0 {
		return resp.RespValue{Type: "null"}
	}

	now := int(time.Now().Unix()) * 1000
	if v.expiry > 0 && v.expiry < now {
		setsMU.Lock()
		delete(sets, key)
		setsMU.Unlock()
		return resp.RespValue{Type: "null"}
	}

	val := v.list[0]
	setsMU.Lock()
	if len(v.list) == 1 {
		delete(sets, key)
	} else {
		v.list = v.list[1:]
		sets[key] = v
	}
	setsMU.Unlock()

	return resp.RespValue{Type: "bulk", Bulk: val}
}
