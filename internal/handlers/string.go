package handlers

import (
	"fmt"
	"time"

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
		}
	}

	if err := h.store.SetKV(kv); err != nil {
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

	now := int(time.Now().Unix()) * 1000

	if v.Exp > 0 && v.Exp < now {
		if _, err := h.store.DeleteByKey(kv); err != nil {
			return handlerResponse{
				err: err,
			}
		}
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

	count := 0
	for _, k := range h.args {
		dc, err := h.store.DeleteByKey(storage.KV{Key: k.Bulk})

		if err != nil {
			return handlerResponse{
				err: err,
			}
		}

		if dc == 1 {
			count++
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

	current.Key = newKey
	h.store.SetKV(current)
	if o.replace {
		if _, err := h.store.DeleteByKey(storage.KV{Key: key}); err != nil {
			return handlerResponse{
				err: err,
			}
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(1),
	}
}
