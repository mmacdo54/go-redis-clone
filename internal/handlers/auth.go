package handlers

import "fmt"

func auth(h handlerArgs) handlerResponse {
	if len(h.args) != 1 {
		return handlerResponse{
			err: fmt.Errorf("Wrong number of args passed to 'auth command'"),
		}
	}

	password := h.args[0].Bulk

	if err := h.config.ValidatePassword(password); err != nil {
		return handlerResponse{
			err: err,
		}
	}

	h.conn.Validated = true
	return handlerResponse{
		resp: generateStringResponse("OK"),
	}
}
