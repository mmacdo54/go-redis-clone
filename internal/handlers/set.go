package handlers

import (
	"fmt"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func sadd(h handlerArgs) resp.RespValue {
	if len(h.args) < 2 {
		return generateErrorResponse(fmt.Errorf("wrong amount of arguments passed to 'sadd' command"))
	}

	key := h.args[0].Bulk

	s, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return generateErrorResponse(err)
	}

	if ok && s.Typ != SET {
		return generateErrorResponse(fmt.Errorf("key is not of type set"))
	}

	if !ok {
		s.Key = key
		s.Typ = SET
		s.Set = map[string]struct{}{}
	}

	count := 0
	for _, m := range h.args[1:] {
		if _, ok := s.Set[m.Bulk]; !ok {
			s.Set[m.Bulk] = struct{}{}
			count++
		}
	}

	if err := h.store.SetKV(s); err != nil {
		return generateErrorResponse(err)
	}

	return resp.RespValue{Type: "integer", Num: count}
}

func smembers(h handlerArgs) resp.RespValue {
	if len(h.args) != 1 {
		return generateErrorResponse(fmt.Errorf("wrong amount of arguments passed to 'smembers' command"))
	}

	key := h.args[0].Bulk
	s, ok, err := h.store.GetByKey(storage.KV{Key: key})

	if err != nil {
		return generateErrorResponse(err)
	}

	if !ok {
		return resp.RespValue{Type: "set"}
	}

	if s.Typ != SET {
		return generateErrorResponse(fmt.Errorf("key is not of type set"))
	}

	members := []resp.RespValue{}

	for k, _ := range s.Set {
		members = append(members, resp.RespValue{Type: "bulk", Bulk: k})
	}

	return resp.RespValue{Type: "set", Array: members}
}
