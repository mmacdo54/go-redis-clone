package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func setExpiry(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return generateErrorResponse(fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(h.command)))
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk
	expiry, err := strconv.Atoi(value)

	if err != nil {
		return generateErrorResponse(invalidOptionsError{})
	}

	var opts options
	if len(h.args) > 2 {
		o, err := parseSetOptions(h.args[2:])
		if err != nil {
			return generateErrorResponse(err)
		}
		opts = o
	}

	v, exists, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return generateErrorResponse(err)
	}

	if !exists {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	if opts.nx && v.Exp != 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	if opts.xx && v.Exp == 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	now := int(time.Now().Unix())
	switch h.command {
	case "EXPIRE":
		v.Exp = (now + expiry) * 1000
	case "EXPIREAT":
		v.Exp = expiry * 1000
	case "PEXPIRE":
		v.Exp = now*1000 + expiry
	case "PEXPIREAT":
		v.Exp = expiry
	default:
		return generateErrorResponse(fmt.Errorf("command '%s' not handled", h.command))
	}

	err = h.store.SetKV(v)

	if err != nil {
		return generateErrorResponse(err)
	}

	return resp.RespValue{Type: "integer", Num: 1}
}

func persist(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return generateErrorResponse(fmt.Errorf("wrong number of args passed to 'persist' command"))
	}

	key := h.args[0].Bulk
	v, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return generateErrorResponse(err)
	}

	if !ok || v.Exp == 0 {
		return resp.RespValue{Type: "integer", Num: 0}
	}

	v.Exp = 0
	if err = h.store.SetKV(v); err != nil {
		return generateErrorResponse(err)
	}

	return resp.RespValue{Type: "integer", Num: 1}
}

func expiretime(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return generateErrorResponse(fmt.Errorf("wrong number of arguments for 'expiretime' command"))
	}

	key := h.args[0].Bulk
	v, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return generateErrorResponse(err)
	}

	if !ok {
		return resp.RespValue{Type: "integer", Num: -2}
	}
	if v.Exp == 0 {
		return resp.RespValue{Type: "integer", Num: -1}
	}

	return resp.RespValue{Type: "integer", Num: v.Exp / 1000}
}
