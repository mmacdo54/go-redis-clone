package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type RespReader struct {
	reader *bufio.Reader
}

func NewRespReader(reader io.Reader) *RespReader {
	return &RespReader{reader: bufio.NewReader(reader)}
}

func (r *RespReader) ReadResp() (RespValue, error) {
	t, err := r.reader.ReadByte()
	if err != nil {
		return RespValue{}, err
	}

	switch t {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		fmt.Printf("Unknown type: %v", string(t))
		return RespValue{}, nil
	}
}

func (r *RespReader) readLine() ([]byte, error) {
	l, err := r.reader.ReadBytes('\n')

	if err != nil {
		return nil, err
	}

	return l[:len(l)-2], nil
}

func (r *RespReader) readInteger() (int, error) {
	i, err := r.readLine()

	if err != nil {
		return 0, err
	}
	res, err := strconv.Atoi(string(i))

	if err != nil {
		return 0, err
	}

	return res, nil
}

func (r *RespReader) readArray() (RespValue, error) {
	val := NewRespValue()
	val.Type = "array"
	length, err := r.readInteger()

	if err != nil {
		return *val, err
	}

	for i := 0; i < length; i++ {
		newVal, err := r.ReadResp()

		if err != nil {
			return *val, err
		}

		val.Array = append(val.Array, newVal)
	}
	return *val, nil
}

func (r *RespReader) readBulk() (RespValue, error) {
	val := NewRespValue()
	val.Type = "bulk"
	if _, err := r.readInteger(); err != nil {
		return *val, err
	}
	bulk, err := r.readLine()
	if err != nil {
		return *val, err
	}

	val.Bulk = string(bulk)
	return *val, nil
}
