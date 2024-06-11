package handlers

import (
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
