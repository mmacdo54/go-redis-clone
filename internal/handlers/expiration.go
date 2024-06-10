package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

func setExpiry(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return resp.RespValue{
			Type: "error",
			Str:  fmt.Sprintf("ERR wrong number of arguments for '%s' command", strings.ToLower(h.command)),
		}
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk
	expiry, err := strconv.Atoi(value)

	if err != nil {
		return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", invalidOptionsError{}.Error())}
	}

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

	if !exists {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	if opts.nx && v.expiry != 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	if opts.xx && v.expiry == 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	now := int(time.Now().Unix())
	switch h.command {
	case "EXPIRE":
		v.expiry = (now + expiry) * 1000
	case "EXPIREAT":
		v.expiry = expiry * 1000
	case "PEXPIRE":
		v.expiry = now*1000 + expiry
	case "PEXPIREAT":
		v.expiry = expiry
	default:
		return resp.RespValue{
			Type: "error",
			Str:  fmt.Sprintf("ERR command '%s' not handled", h.command),
		}
	}

	setsMU.Lock()
	sets[key] = v
	setsMU.Unlock()

	return resp.RespValue{Type: "integer", Num: 1}
}

func persist(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of args passed to 'persist' command"}
	}

	key := h.args[0].Bulk
	setsMU.RLock()
	s, ok := sets[key]
	if !ok || s.expiry == 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}
	setsMU.RUnlock()

	setsMU.Lock()
	s.expiry = 0
	sets[key] = s
	setsMU.Unlock()

	return resp.RespValue{Type: "integer", Num: 1}
}

func expiretime(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return resp.RespValue{Type: "error", Str: "ERR wrong number of arguments for 'expiretime' command"}
	}

	key := h.args[0].Bulk
	setsMU.RLock()
	v, ok := sets[key]
	setsMU.RUnlock()

	if !ok {
		return resp.RespValue{Type: "integer", Num: -2}
	}
	if v.expiry == 0 {
		return resp.RespValue{Type: "integer", Num: -1}
	}

	return resp.RespValue{Type: "integer", Num: v.expiry / 1000}
}
