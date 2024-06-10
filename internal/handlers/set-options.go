package handlers

import (
	"slices"
	"strings"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

func parseSetOptions(opts []resp.RespValue) (options, error) {
	s := options{}
	if len(opts) == 0 {
		return s, nil
	}

	if err := s.setNXOrXXOption(opts); err != nil {

		return s, err
	}

	if err := s.setTTLOptions(opts); err != nil {
		return s, err
	}

	if slices.ContainsFunc(opts, func(rv resp.RespValue) bool {
		return strings.ToUpper(rv.Bulk) == GET
	}) {
		s.get = true
	}

	return s, nil
}

func parseExpireOptions(opts []resp.RespValue) (options, error) {
	s := options{}
	if len(opts) == 0 {
		return s, nil
	}

	if err := s.setNXOrXXOption(opts); err != nil {
		return s, err
	}

	if err := s.setLTorGTOptions(opts); err != nil {
		return s, err
	}

	if s.nx && (s.lt || s.gt) {
		return s, invalidOptionsError{}
	}

	return s, nil
}
