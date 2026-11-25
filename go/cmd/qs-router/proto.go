package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Startup-phase request codes (the "protocol version" field of an untyped
// first message).
const (
	codeSSLRequest    = 80877103
	codeGSSEncRequest = 80877104
	codeCancelRequest = 80877102
)

const maxMessage = 1 << 30

// pipe is one side of the proxy. Messages are relayed as raw frames; only the
// ones the router has to understand are decoded, with pgproto3.
type pipe struct {
	c   net.Conn
	r   *bufio.Reader
	w   *bufio.Writer
	hdr [5]byte
	buf []byte
	enc []byte
}

func newPipe(c net.Conn) *pipe {
	return &pipe{c: c, r: bufio.NewReaderSize(c, 32<<10), w: bufio.NewWriterSize(c, 32<<10)}
}

// read returns the next typed message. body is only valid until the next read.
func (p *pipe) read() (typ byte, body []byte, err error) {
	if _, err = io.ReadFull(p.r, p.hdr[:]); err != nil {
		return 0, nil, err
	}
	n := int(binary.BigEndian.Uint32(p.hdr[1:])) - 4
	if n < 0 || n > maxMessage {
		return 0, nil, fmt.Errorf("message %q with length %d", p.hdr[0], n+4)
	}
	if cap(p.buf) < n {
		p.buf = make([]byte, n, n+n/4)
	}
	p.buf = p.buf[:n]
	if _, err = io.ReadFull(p.r, p.buf); err != nil {
		return 0, nil, err
	}
	return p.hdr[0], p.buf, nil
}

func (p *pipe) write(typ byte, body []byte) error {
	var h [5]byte
	h[0] = typ
	binary.BigEndian.PutUint32(h[1:], uint32(len(body)+4))
	if _, err := p.w.Write(h[:]); err != nil {
		return err
	}
	_, err := p.w.Write(body)
	return err
}

func (p *pipe) raw(b []byte) error {
	_, err := p.w.Write(b)
	return err
}

type encoder interface {
	Encode(dst []byte) ([]byte, error)
}

func (p *pipe) send(msgs ...encoder) error {
	p.enc = p.enc[:0]
	var err error
	for _, m := range msgs {
		if p.enc, err = m.Encode(p.enc); err != nil {
			return err
		}
	}
	return p.raw(p.enc)
}

func (p *pipe) flush() error { return p.w.Flush() }

// frame appends one raw message to dst.
func frame(dst []byte, typ byte, body []byte) []byte {
	dst = append(dst, typ, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(dst[len(dst)-4:], uint32(len(body)+4))
	return append(dst, body...)
}

func cstring(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

func pgError(code, msg string) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: code, Message: msg}
}

// readStartup reads an untyped startup-phase message: length, then payload.
func readStartup(r io.Reader) ([]byte, error) {
	var l [4]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(l[:]))
	if n < 8 || n > 1<<20 {
		return nil, fmt.Errorf("startup message of length %d", n)
	}
	b := make([]byte, n)
	copy(b, l[:])
	if _, err := io.ReadFull(r, b[4:]); err != nil {
		return nil, err
	}
	return b, nil
}
