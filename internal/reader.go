package grc

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

type Reader struct {
	reader *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{bufio.NewReader(r)}
}

func (r *Reader) Read() (RespValue, error) {
	b, err := r.reader.ReadByte()
	if err != nil {
		return RespValue{}, err
	}

	switch b {
	case ARRAY:
		return r.readArray()
	case BULK:
		return r.readBulk()
	default:
		return RespValue{}, fmt.Errorf("Unknown type: %v", string(b))
	}

}

func (r *Reader) readLine() ([]byte, error) {
	l, err := r.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	return l[:len(l)-2], nil
}

func (r *Reader) readInteger() (int, error) {
	l, err := r.readLine()
	if err != nil {
		return 0, err
	}

	i, err := strconv.Atoi(string(l))
	if err != nil {
		return 0, err
	}

	return i, nil
}

func (r *Reader) readArray() (RespValue, error) {
	rv := NewRespValue()
	rv.Type = "array"

	i, err := r.readInteger()
	if err != nil {
		return rv, nil
	}

	for j := 0; j < i; j++ {
		l, err := r.Read()

		if err != nil {
			return rv, err
		}

		rv.Arr = append(rv.Arr, l)
	}

	return rv, nil
}

func (r *Reader) readBulk() (RespValue, error) {
	rv := NewRespValue()
	rv.Type = "bulk"

	if _, err := r.readInteger(); err != nil {
		return rv, err
	}

	l, err := r.readLine()
	if err != nil {
		return rv, err
	}
	rv.Bulk = string(l)

	return rv, nil
}
