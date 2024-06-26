package connection

import "net"

type Connection struct {
	Conn      *net.Conn
	Validated bool
}

func NewConnection(conn *net.Conn) Connection {
	return Connection{Conn: conn}
}
