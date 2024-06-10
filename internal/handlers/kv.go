package handlers

import (
	"fmt"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

func exists(h handlerArgs) resp.RespValue {
	if len(h.args) == 0 {
		return resp.RespValue{Type: "error", Str: "ERR no keys passed to 'exists' command"}
	}

	count := 0
	for _, k := range h.args {
		setsMU.RLock()
		if _, ok := sets[k.Bulk]; ok {
			count++
		}
		setsMU.RUnlock()
	}

	return resp.RespValue{Type: "integer", Num: count}
}

func set(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'set' command"}
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk
	var opts options
	if len(h.args) > 2 {
		o, err := parseSetOptions(h.args[2:])
		if err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err.Error())}
		}
		opts = o
	}

	setsMU.RLock()
	v, exists := sets[key]
	setsMU.RUnlock()

	if opts.nx && exists {
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

	setsMU.Lock()
	sets[key] = s
	setsMU.Unlock()

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

	setsMU.RLock()
	value, ok := sets[key]
	setsMU.RUnlock()

	if !ok {
		return resp.RespValue{Type: "null"}
	}

	now := int(time.Now().Unix()) * 1000

	if value.expiry > 0 && value.expiry < now {
		setsMU.Lock()
		delete(sets, key)
		setsMU.Unlock()
		return resp.RespValue{Type: "null"}
	}

	return resp.RespValue{Type: "bulk", Bulk: value.value}
}

func del(h handlerArgs) resp.RespValue {
	if len(h.args) == 0 {
		return resp.RespValue{Type: "error", Str: "ERR no keys passed to 'del' command"}
	}

	count := 0
	for _, k := range h.args {
		setsMU.Lock()
		if _, ok := sets[k.Bulk]; ok {
			count++
			delete(sets, k.Bulk)
		}
		setsMU.Unlock()
	}

	return resp.RespValue{Type: "integer", Num: count}
}

func copy(h handlerArgs) resp.RespValue {
	if len(h.args) == 0 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of commands passed to 'copy' command"}
	}

	key := h.args[0].Bulk
	newKey := h.args[1].Bulk
	o := parseCopyOptions(h.args)
	fmt.Println(o.replace)
	setsMU.RLock()
	current, oldExists := sets[key]
	_, newExists := sets[newKey]
	setsMU.RUnlock()
	if !oldExists || newExists {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	setsMU.Lock()
	sets[newKey] = current
	if o.replace {
		delete(sets, key)
	}
	setsMU.Unlock()

	return resp.RespValue{Type: "integer", Num: 1}
}
