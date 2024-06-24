package handlers

import (
	"fmt"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func sadd(h handlerArgs) handlerResponse {
	if len(h.args) < 2 {
		return handlerResponse{
			err: fmt.Errorf("wrong amount of arguments passed to 'sadd' command"),
		}
	}

	key := h.args[0].Bulk

	s, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if ok && s.Typ != SET {
		return handlerResponse{
			err: fmt.Errorf("key is not of type set"),
		}
	}

	if !ok {
		s.Key = key
		s.Typ = SET
		s.Set = map[string]interface{}{}
	}

	count := 0
	for _, m := range h.args[1:] {
		if _, ok := s.Set[m.Bulk]; !ok {
			s.Set[m.Bulk] = struct{}{}
			count++
		}
	}

	tx, err := h.store.InitTransaction()
	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if err := h.store.SetKV(s, tx); err != nil {
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
		resp: generateIntegerResponse(count),
	}
}

func smembers(h handlerArgs) handlerResponse {
	if len(h.args) != 1 {
		return handlerResponse{
			err: fmt.Errorf("wrong amount of arguments passed to 'smembers' command"),
		}
	}

	key := h.args[0].Bulk
	s, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return handlerResponse{
			err: err,
		}
	}

	if !ok {
		return handlerResponse{
			resp: generateSetResponse([]resp.RespValue{}),
		}
	}

	if s.Typ != SET {
		return handlerResponse{
			err: fmt.Errorf("key is not of type set"),
		}
	}

	members := []resp.RespValue{}
	for k, _ := range s.Set {
		members = append(members, generateBulkResponse(k))
	}

	return handlerResponse{
		resp: generateSetResponse(members),
	}
}
