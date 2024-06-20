package resp

import "strconv"

const (
	TYPE_ARRAY   = "array"
	TYPE_INTEGER = "integer"
	TYPE_BULK    = "bulk"
	TYPE_ERROR   = "error"
	TYPE_STRING  = "string"
	TYPE_NULL    = "null"
	TYPE_SET     = "set"
	TYPE_VOID    = "void"
)

type RespValue struct {
	Type  string
	Str   string
	Num   int
	Bulk  string
	Array []RespValue
}

func NewRespValue() *RespValue {
	return &RespValue{}
}

func (v RespValue) Marshall() []byte {
	switch v.Type {
	case TYPE_ARRAY:
		return v.marshalArray()
	case TYPE_BULK:
		return v.marshalBulk()
	case TYPE_ERROR:
		return v.marshalError()
	case TYPE_INTEGER:
		return v.marshalInteger()
	case TYPE_STRING:
		return v.marshalString()
	case TYPE_NULL:
		return v.marshalNull()
	case TYPE_SET:
		return v.marshallSet()
	default:
		return []byte{}
	}
}

func addRespReturn(line *[]byte) {
	*line = append(*line, '\r', '\n')
}

func (v RespValue) marshalString() (res []byte) {
	res = append(res, STRING)
	res = append(res, v.Str...)
	addRespReturn(&res)
	return
}

func (v RespValue) marshalBulk() (res []byte) {
	res = append(res, BULK)
	res = append(res, strconv.Itoa(len(v.Bulk))...)
	addRespReturn(&res)
	res = append(res, v.Bulk...)
	addRespReturn(&res)
	return
}

func (v RespValue) marshalError() (res []byte) {
	res = append(res, ERROR)
	res = append(res, v.Str...)
	addRespReturn(&res)
	return
}

func (v RespValue) marshalInteger() (res []byte) {
	res = append(res, INTEGER)
	res = append(res, strconv.Itoa(v.Num)...)
	addRespReturn(&res)
	return
}

func (v RespValue) marshalArray() (res []byte) {
	res = append(res, ARRAY)
	res = append(res, strconv.Itoa(len(v.Array))...)
	addRespReturn(&res)

	for _, val := range v.Array {
		bytes := val.Marshall()
		res = append(res, bytes...)
	}

	return
}

func (v RespValue) marshalNull() (res []byte) {
	return []byte("$-1\r\n")
}

func (v RespValue) marshallSet() (res []byte) {
	res = append(res, SET)
	res = append(res, strconv.Itoa(len(v.Array))...)
	addRespReturn(&res)

	for _, val := range v.Array {
		bytes := val.Marshall()
		res = append(res, bytes...)
	}

	return
}
