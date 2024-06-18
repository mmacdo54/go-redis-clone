package handlers

import (
	"fmt"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func exists(h handlerArgs) resp.RespValue {
	if len(h.args) == 0 {
		return resp.RespValue{Type: "error", Str: "ERR no keys passed to 'exists' command"}
	}

	count := 0
	for _, k := range h.args {
		exists, err := h.store.Exists(storage.KV{Key: k.Bulk})
		if err != nil {
			return resp.RespValue{Type: "error", Str: fmt.Sprintf("ERR %s", err)}
		}

		if exists {
			count++
		}
	}

	return resp.RespValue{Type: "integer", Num: count}
}
