package handlers

import (
	"fmt"

	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func set(h handlerArgs) handlerResponse {
	if len(h.args) < 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of arguments for 'set' command"),
		}
	}

	key := h.args[0].Bulk
	value := h.args[1].Bulk
	var opts options
	if len(h.args) > 2 {
		o, err := parseSetOptions(h.args[2:])
		if err != nil {
			return handlerResponse{
				err: err,
			}
		}
		opts = o
	}

	v, exists, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if opts.nx && exists {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	if opts.xx && !exists {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	kv := storage.KV{Typ: STRING, Key: key, Str: value}

	if opts.keepttl && exists {
		kv.Exp = v.Exp
	} else {
		switch {
		case opts.ex > 0:
			kv.Exp = opts.ex
		case opts.px > 0:
			kv.Exp = opts.px
		case opts.exat > 0:
			kv.Exp = opts.exat
		case opts.pxat > 0:
			kv.Exp = opts.pxat
		default:
			kv.Exp = 0
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if err := h.store.SetKV(kv, tx); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if opts.get && !exists {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}
	if opts.get {
		if v.Typ != STRING {
			return handlerResponse{
				err: fmt.Errorf("value stored at key is not a string"),
			}
		}
		return handlerResponse{
			resp: generateBulkResponse(v.Str),
		}
	}

	if err := tx.Commit(); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	return handlerResponse{
		resp: generateStringResponse("OK"),
	}
}

func get(h handlerArgs) handlerResponse {
	if len(h.args) != 1 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of arguments for 'get' command"),
		}
	}

	key := h.args[0].Bulk
	kv := storage.KV{Key: key}

	v, exists, err := h.store.GetByKey(kv)

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !exists {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	if v.Typ != STRING {
		return handlerResponse{
			err: fmt.Errorf("value stored at key is not a string"),
		}
	}

	return handlerResponse{
		resp: generateBulkResponse(v.Str),
	}
}

func del(h handlerArgs) handlerResponse {
	if len(h.args) == 0 {
		return handlerResponse{
			err: fmt.Errorf("no keys passed to 'del' command"),
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}
	count := 0
	for _, k := range h.args {
		dc, err := h.store.DeleteByKey(storage.KV{Key: k.Bulk}, tx)

		if err != nil {
			return handlerResponse{
				err: err,
			}
		}

		if dc == 1 {
			count++
		}
	}

	if err := tx.Commit(); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(count),
	}
}

func copy(h handlerArgs) handlerResponse {
	if len(h.args) == 0 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of commands passed to 'copy' command"),
		}
	}

	key := h.args[0].Bulk
	newKey := h.args[1].Bulk
	o := parseCopyOptions(h.args)

	current, oldExists, err := h.store.GetByKey(storage.KV{Key: key})
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	_, newExists, err := h.store.GetByKey(storage.KV{Key: newKey})
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !oldExists || newExists {
		return handlerResponse{
			resp: generateIntegerResponse(0),
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	current.Key = newKey
	if err := h.store.SetKV(current, tx); err != nil {
		return handlerResponse{
			err: err,
		}
	}
	if o.replace {
		if _, err := h.store.DeleteByKey(storage.KV{Key: key}, tx); err != nil {
			return handlerResponse{
				err: err,
			}
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
