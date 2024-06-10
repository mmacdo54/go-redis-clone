package handlers

import (
	"slices"
	"strings"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

func parseSetOptions(opts []resp.RespValue) (options, error) {
	s := newOptions(opts)
	if len(opts) == 0 {
		return s, nil
	}

	if err := s.setNXOrXXOption(); err != nil {
		return s, err
	}

	if err := s.setTTLOptions(); err != nil {
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
	s := newOptions(opts)
	if len(opts) == 0 {
		return s, nil
	}

	if err := s.setNXOrXXOption(); err != nil {
		return s, err
	}

	if err := s.setLTorGTOptions(); err != nil {
		return s, err
	}

	if s.nx && (s.lt || s.gt) {
		return s, invalidOptionsError{}
	}

	return s, nil
}

func parseCopyOptions(opts []resp.RespValue) options {
	s := newOptions(opts)
	s.setReplaceOption()
	return s
}
