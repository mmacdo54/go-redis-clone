package handlers

import (
	"fmt"

	"github.com/mmacdo54/go-redis-clone/internal/storage"
)

func exists(h handlerArgs) handlerResponse {
	if len(h.args) == 0 {
		return handlerResponse{
			err: fmt.Errorf("no keys passed to 'exists' command"),
		}
	}

	count := 0
	for _, k := range h.args {
		exists, err := h.store.Exists(storage.KV{Key: k.Bulk})
		if err != nil {
			return handlerResponse{
				err: err,
			}
		}

		if exists {
			count++
		}
	}

	return handlerResponse{
		resp: generateIntegerResponse(count),
	}
}
