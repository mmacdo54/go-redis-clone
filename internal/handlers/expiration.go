package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func setExpiry(h handlerArgs) handlerResponse {
	if len(h.args) < 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of arguments for '%s' command", strings.ToLower(h.command)),
		}
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk
	expiry, err := strconv.Atoi(value)

	if err != nil {
		return handlerResponse{err: invalidOptionsError{}}
	}

	var opts options
	if len(h.args) > 2 {
		o, err := parseSetOptions(h.args[2:])
		if err != nil {
			return handlerResponse{err: err}
		}
		opts = o
	}

	v, exists, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{err: err}
	}

	if !exists {
		return handlerResponse{
			resp: generateIntegerResponse(0),
		}
	}

	if opts.nx && v.Exp != 0 {
		return handlerResponse{
			resp: generateIntegerResponse(0),
		}
	}

	if opts.xx && v.Exp == 0 {
		return handlerResponse{
			resp: generateIntegerResponse(0),
		}
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
		return handlerResponse{
			err: fmt.Errorf("command '%s' not handled", h.command),
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	err = h.store.SetKV(v, tx)

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if err := tx.Commit(); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(1),
	}
}

func persist(h handlerArgs) handlerResponse {
	if len(h.args) != 1 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of args passed to 'persist' command"),
		}
	}

	key := h.args[0].Bulk
	v, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok || v.Exp == 0 {
		return handlerResponse{
			resp: generateIntegerResponse(0),
		}
	}

	v.Exp = 0
	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if err = h.store.SetKV(v, tx); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if err := tx.Commit(); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(1),
	}
}

func expiretime(h handlerArgs) handlerResponse {
	if len(h.args) != 1 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of arguments for 'expiretime' command"),
		}
	}

	key := h.args[0].Bulk
	v, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok {
		return handlerResponse{
			resp: generateIntegerResponse(-2),
		}
	}
	if v.Exp == 0 {
		return handlerResponse{
			resp: generateIntegerResponse(-1),
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(v.Exp / 1000),
	}
}
