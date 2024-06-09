package resp

import (
	"io"
)

type RespWriter struct {
	writer io.Writer
}

func NewRespWriter(writer io.Writer) *RespWriter {
	return &RespWriter{writer}
}

func (w *RespWriter) WriteErrorResp(e error) error {
	v := NewRespValue()
	v.Type = "error"
	v.Str = e.Error()
	bytes := v.Marshall()

	if _, err := w.writer.Write(bytes); err != nil {
		return err
	}

	return nil
}

func (w *RespWriter) WriteResp(v RespValue) error {
	bytes := v.Marshall()

	if _, err := w.writer.Write(bytes); err != nil {
		return err
	}

	return nil
}
