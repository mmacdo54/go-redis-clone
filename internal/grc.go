package grc

const (
	ARRAY = '*'
	BULK  = '$'
)

type RespValue struct {
	Type string
	Bulk string
	Arr  []RespValue
}

func NewRespValue() RespValue {
	return RespValue{}
}
