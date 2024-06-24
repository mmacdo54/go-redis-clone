package handlers

import (
	"fmt"
	"strconv"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func lpush(h handlerArgs) handlerResponse {
	if len(h.args) < 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of commands passed to '%s' command", h.command),
		}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if h.command == "LPUSHX" && !ok {
		return handlerResponse{
			err: fmt.Errorf("key does not exist"),
		}
	}

	if ok && el.Typ != "" && el.Typ != LIST {
		return handlerResponse{
			err: fmt.Errorf("value stored at key is not a list"),
		}
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

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}
	err = h.store.SetKV(el, tx)

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
		resp: generateIntegerResponse(len(list)),
	}
}

func rpush(h handlerArgs) handlerResponse {
	if len(h.args) < 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of commands passed to '%s' command", h.command),
		}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if h.command == "RPUSHX" && !ok {
		return handlerResponse{
			err: fmt.Errorf("key does not exist"),
		}
	}

	if ok && el.Typ != "" && el.Typ != LIST {
		return handlerResponse{
			err: fmt.Errorf("value stored at key is not a list"),
		}
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

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}
	err = h.store.SetKV(el, tx)

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
		resp: generateIntegerResponse(len(el.Arr)),
	}
}

func lpop(h handlerArgs) handlerResponse {
	// TODO handle a range
	if len(h.args) == 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of commands passed to 'lpop' command"),
		}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok || el.Typ != LIST || len(el.Arr) == 0 {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	now := int(time.Now().Unix()) * 1000
	if el.Exp > 0 && el.Exp < now {
		if _, err := h.store.DeleteByKey(el, tx); err != nil {
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
			resp: generateNullResponse(),
		}
	}

	val := el.Arr[0]
	if len(el.Arr) == 1 {
		if _, err := h.store.DeleteByKey(el, tx); err != nil {
			return handlerResponse{
				err: err,
			}
		}
	} else {
		el.Arr = el.Arr[1:]
		if err := h.store.SetKV(el, tx); err != nil {
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
		resp: generateBulkResponse(val),
	}
}

func rpop(h handlerArgs) handlerResponse {
	// TODO handle a range
	if len(h.args) == 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong number of commands passed to 'rpop' command"),
		}
	}

	key := h.args[0].Bulk
	el, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok || el.Typ != LIST || len(el.Arr) == 0 {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	now := int(time.Now().Unix()) * 1000
	if el.Exp > 0 && el.Exp < now {
		if _, err := h.store.DeleteByKey(el, tx); err != nil {
			return handlerResponse{
				err: err,
			}
		}
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	val := el.Arr[len(el.Arr)-1]
	if len(el.Arr) == 1 {
		if _, err := h.store.DeleteByKey(el, tx); err != nil {
			return handlerResponse{
				err: err,
			}
		}
	} else {
		el.Arr = el.Arr[:len(el.Arr)-1]
		if err := h.store.SetKV(el, tx); err != nil {
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
		resp: generateBulkResponse(val),
	}
}

func llen(h handlerArgs) handlerResponse {
	if len(h.args) == 0 {
		return handlerResponse{
			err: fmt.Errorf("no key passed to 'llen' command"),
		}
	}

	key := h.args[0].Bulk

	l, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok {
		return handlerResponse{
			resp: generateIntegerResponse(0),
		}
	}

	if l.Typ != LIST {
		return handlerResponse{
			err: fmt.Errorf("value at key is not a list"),
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(len(l.Arr)),
	}
}

func lindex(h handlerArgs) handlerResponse {
	if len(h.args) != 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong amount of arguments to 'lindex' command"),
		}
	}

	key := h.args[0].Bulk
	index, err := strconv.Atoi(h.args[1].Bulk)

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	l, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok {
		return handlerResponse{
			resp: generateNullResponse(),
		}
	}

	if l.Typ != LIST {
		return handlerResponse{
			err: fmt.Errorf("value at key is not a list"),
		}
	}

	if index < 0 {
		index = len(l.Arr) + index
	}

	if index < 0 || index >= len(l.Arr) {
		return handlerResponse{
			err: fmt.Errorf("index out of range"),
		}
	}

	return handlerResponse{
		resp: generateBulkResponse(l.Arr[index]),
	}
}
