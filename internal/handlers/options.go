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
	LT      = "LT"
	GT      = "GT"
	REPLACE = "REPLACE"
)

type options struct {
	nx      bool
	xx      bool
	keepttl bool
	get     bool
	ex      int
	px      int
	exat    int
	pxat    int
	lt      bool
	gt      bool
	replace bool
	opts    []resp.RespValue
}

type invalidOptionsError struct{}

func (i invalidOptionsError) Error() string {
	return "invalid options sent to 'set' command"
}

func newOptions(opts []resp.RespValue) options {
	return options{opts: opts}
}

func (o *options) setNXOrXXOption() error {
	filteredOptions := []resp.RespValue{}

	for _, opt := range o.opts {
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
		o.nx = true
		return nil
	}

	o.xx = true
	return nil
}

func (o *options) setTTLOptions() error {
	filteredOptions := []resp.RespValue{}

	for _, opt := range o.opts {
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
		o.keepttl = true
		return nil
	}

	i := slices.IndexFunc(o.opts, func(rv resp.RespValue) bool {
		return strings.ToUpper(rv.Bulk) == opt
	})

	if len(o.opts)-1 < i+1 {
		return invalidOptionsError{}
	}

	t, err := strconv.Atoi(o.opts[i+1].Bulk)

	if err != nil {
		return invalidOptionsError{}
	}

	now := int(time.Now().Unix())

	switch opt {
	case EX:
		{
			o.ex = (now + t) * 1000
			return nil
		}
	case PX:
		{
			o.px = now*1000 + t
			return nil
		}
	case EXAT:
		{
			o.exat = t * 1000
			return nil
		}
	case PXAT:
		{
			o.pxat = t
			return nil
		}
	}

	return invalidOptionsError{}
}

func (o *options) setLTorGTOptions() error {
	filteredOptions := []resp.RespValue{}

	for _, opt := range o.opts {
		upper := strings.ToUpper(opt.Bulk)
		if upper == LT || upper == GT {
			filteredOptions = append(filteredOptions, opt)
		}
	}

	if len(filteredOptions) == 0 {
		return nil
	}

	if len(filteredOptions) > 1 {
		return invalidOptionsError{}
	}

	if strings.ToUpper(filteredOptions[0].Bulk) == LT {
		o.lt = true
		return nil
	}

	o.gt = true
	return nil
}

func (o *options) setReplaceOption() {
	if slices.ContainsFunc(o.opts, func(rv resp.RespValue) bool {
		return strings.ToUpper(rv.Bulk) == REPLACE
	}) {
		o.replace = true
	}
}
