package handlers

import (
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mmacdo54/go-redis-clone/internal/resp"
)

const (
	NX      = "NX"
	XX      = "XX"
	KEEPTTL = "KEEPTTL"
	GET     = "GET"
	EX      = "EX"
	PX      = "PX"
	EXAT    = "EXAT"
	PXAT    = "PXAT"
)

type setOptions struct {
	nx      bool
	xx      bool
	keepttl bool
	get     bool
	ex      int
	px      int
	exat    int
	pxat    int
}

type invalidOptionsError struct{}

func (i invalidOptionsError) Error() string {
	return "invalid options sent to 'set' command"
}

func setNXOrXXOption(opts []resp.RespValue, s *setOptions) error {
	filteredOptions := []resp.RespValue{}

	for _, opt := range opts {
		upper := strings.ToUpper(opt.Bulk)
		if upper == XX || upper == NX {
			filteredOptions = append(filteredOptions, opt)
		}
	}

	if len(filteredOptions) == 0 {
		return nil
	}

	if len(filteredOptions) > 1 {
		return invalidOptionsError{}
	}

	if strings.ToUpper(filteredOptions[0].Bulk) == NX {
		s.nx = true
		return nil
	}

	s.xx = true
	return nil
}

func setTTLOptions(opts []resp.RespValue, s *setOptions) error {
	filteredOptions := []resp.RespValue{}

	for _, opt := range opts {
		if slices.Contains([]string{KEEPTTL, EX, PX, EXAT, PXAT}, strings.ToUpper(opt.Bulk)) {
			filteredOptions = append(filteredOptions, opt)
		}
	}

	if len(filteredOptions) == 0 {
		return nil
	}

	if len(filteredOptions) > 1 {
		return invalidOptionsError{}
	}

	opt := strings.ToUpper(filteredOptions[0].Bulk)

	if strings.ToUpper(opt) == KEEPTTL {
		s.keepttl = true
		return nil
	}

	i := slices.IndexFunc(opts, func(rv resp.RespValue) bool {
		return strings.ToUpper(rv.Bulk) == opt
	})

	if len(opts)-1 < i+1 {
		return invalidOptionsError{}
	}

	t, err := strconv.Atoi(opts[i+1].Bulk)

	if err != nil {
		return invalidOptionsError{}
	}

	now := int(time.Now().Unix())

	switch opt {
	case EX:
		{
			s.ex = (now + t) * 1000
			return nil
		}
	case PX:
		{
			s.px = now*1000 + t
			return nil
		}
	case EXAT:
		{
			s.exat = t * 1000
			return nil
		}
	case PXAT:
		{
			s.pxat = t
			return nil
		}
	}

	return invalidOptionsError{}
}

func parseSetOptions(opts []resp.RespValue) (setOptions, error) {
	s := setOptions{}
	if len(opts) == 0 {
		return s, nil
	}

	if err := setNXOrXXOption(opts, &s); err != nil {

		return s, err
	}

	if err := setTTLOptions(opts, &s); err != nil {
		return s, err
	}

	if slices.ContainsFunc(opts, func(rv resp.RespValue) bool {
		return strings.ToUpper(rv.Bulk) == GET
	}) {
		s.get = true
	}

	return s, nil
}
