// qs-router is an experimental PostgreSQL wire-protocol proxy that sends each
// statement to the heap or to the columnar mirror of the same tables.
//
// It does not rewrite SQL. The mirror's views live in their own schema under
// the heap tables' names, and "to the mirror" means: put that schema in front
// of search_path for this one statement. Which side is PostgreSQL's own
// opinion: the statement is planned against the heap first (EXPLAIN, never
// executed) and goes to the mirror only when that plan reads mirrored tables
// and nothing else, and is big enough to be worth it.
//
// See docs/38-a-router-in-front.md for what it does not do.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type config struct {
	listen       string
	backend      string
	heapSchema   string
	mirrorSchema string
	th           Thresholds
	allowOrderBy bool
	heapWords    []string
	provider     string
	logRoutes    bool
}

type server struct {
	cfg   config
	cache *decisionCache
	cats  *catalogCache
	fresh *freshness
}

func (srv *server) logRoute(last string, infos []*stmtInfo) {
	if !srv.cfg.logRoutes {
		return
	}
	text := ""
	for i, in := range infos {
		if i > 0 {
			text += " ;; "
		}
		text += in.Exact
	}
	if len(text) > 200 {
		text = text[:200]
	}
	log.Printf("route %s <- %s", last, text)
}

func (srv *server) dial() (net.Conn, error) {
	if strings.HasPrefix(srv.cfg.backend, "/") {
		return net.DialTimeout("unix", srv.cfg.backend, 5*time.Second)
	}
	return net.DialTimeout("tcp", srv.cfg.backend, 5*time.Second)
}

func (srv *server) handle(c net.Conn) {
	defer c.Close()
	var startup []byte
	for {
		b, err := readStartup(c)
		if err != nil {
			return
		}
		switch binary.BigEndian.Uint32(b[4:8]) {
		case codeSSLRequest, codeGSSEncRequest:
			// Refused. The router relays the authentication exchange in the
			// clear and has no certificate; a client that insists (sslmode=
			// require) disconnects, which is the right outcome.
			if _, err := c.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		case codeCancelRequest:
			// The client's BackendKeyData came straight from the backend, so
			// the request is valid there as it stands.
			if be, err := srv.dial(); err == nil {
				_, _ = be.Write(b)
				_, _ = io.Copy(io.Discard, be)
				be.Close()
			}
			return
		}
		startup = b
		break
	}

	var sm pgproto3.StartupMessage
	if err := sm.Decode(startup[4:]); err != nil {
		// Not protocol 3: hand it to the server unread and let it say so.
		sm.Parameters = map[string]string{}
	}
	bc, err := srv.dial()
	if err != nil {
		buf, _ := (&pgproto3.ErrorResponse{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: "08006",
			Message: "qs-router: cannot reach the server: " + err.Error()}).Encode(nil)
		_, _ = c.Write(buf)
		return
	}
	defer bc.Close()
	if _, err := bc.Write(startup); err != nil {
		return
	}
	if _, repl := sm.Parameters["replication"]; repl || sm.Parameters == nil || len(sm.Parameters) == 0 {
		// Replication (CopyBoth, walsender commands) is not routed at all.
		go func() { _, _ = io.Copy(bc, c); bc.Close() }()
		_, _ = io.Copy(c, bc)
		return
	}

	s := &session{srv: srv, cl: newPipe(c), be: newPipe(bc), mode: "auto", stmts: map[string]*prepared{}, tx: 'I'}
	s.db = sm.Parameters["database"]
	if s.db == "" {
		s.db = sm.Parameters["user"]
	}
	if m := startupRoute(sm.Parameters); m != "" {
		s.mode = m
	}

	// Authentication is relayed, not performed: the router never holds a
	// credential. Every request the server makes that needs an answer is
	// followed by exactly one message from the client.
	for authed := false; !authed; {
		typ, body, err := s.be.read()
		if err != nil {
			return
		}
		if err := s.cl.write(typ, body); err != nil {
			return
		}
		switch typ {
		case 'R':
			if code := be32(body); code != 0 && code != 12 { // not Ok, not SASLFinal
				if err := s.cl.flush(); err != nil {
					return
				}
				t, b, err := s.cl.read()
				if err != nil {
					return
				}
				if err := s.be.write(t, b); err != nil {
					return
				}
				if err := s.be.flush(); err != nil {
					return
				}
			}
		case 'S':
			s.paramStatus(body, false)
		case 'E':
			_ = s.cl.flush()
			return
		case 'Z':
			s.tx = body[0]
			if err := s.cl.flush(); err != nil {
				return
			}
			authed = true
		}
	}
	if err := s.serve(); err != nil && err != io.EOF && srv.cfg.logRoutes {
		log.Printf("session ended: %v", err)
	}
}

var routeOpt = regexp.MustCompile(`(?:-c\s*|--)qs\.route=(\w+)`)

// startupRoute reads qs.route from the startup packet, where a driver puts
// settings it was given in the connection string.
func startupRoute(p map[string]string) string {
	v := p["qs.route"]
	if m := routeOpt.FindStringSubmatch(p["options"]); m != nil {
		v = m[1]
	}
	switch strings.ToLower(v) {
	case "":
		return ""
	case "auto", "heap", "mirror":
		return strings.ToLower(v)
	}
	return "heap"
}

var identRE = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func main() {
	var cfg config
	var ttl time.Duration
	var lsnSpec string
	var maxLag uint64
	flag.StringVar(&cfg.listen, "listen", "127.0.0.1:6432", "address to accept clients on")
	flag.StringVar(&cfg.backend, "backend", "127.0.0.1:5432", "PostgreSQL to proxy to: host:port, or the path of a unix socket")
	flag.StringVar(&cfg.heapSchema, "heap-schema", "public", "schema of the mirrored heap tables")
	flag.StringVar(&cfg.mirrorSchema, "mirror-schema", "qs_mirror", "schema holding the mirror's views, named as the heap tables are")
	flag.Float64Var(&cfg.th.Rows, "row-threshold", 50000, "send to the mirror when a scan of a mirrored table is expected to read more rows than this")
	flag.Float64Var(&cfg.th.Cost, "cost-threshold", 10000, "or when the plan's total cost is above this")
	flag.DurationVar(&ttl, "cache-ttl", time.Minute, "how long a decision is reused; 0 plans every statement")
	flag.StringVar(&lsnSpec, "mirror-lsn", "", "where the mirror's applied LSN is read: http://sidecar/readyz or file:/mirror/schema.table/state.json (read with the mirror's watermark.json, so a quiet table is not stale); empty disables the freshness gate")
	flag.Uint64Var(&maxLag, "max-lag-bytes", 16<<20, "do not use a mirror more than this many WAL bytes behind the server")
	flag.BoolVar(&cfg.allowOrderBy, "allow-order-by", false, "let statements with ORDER BY go to the mirror (DuckDB orders NULLs and text differently)")
	var words string
	// Each of these was measured to answer differently through pg_duckdb 1.2.0:
	// avg(numeric) comes back as a double, pg_typeof() names DuckDB's types,
	// current_setting('search_path') is empty. The list is what was found, not
	// what exists.
	flag.StringVar(&words, "heap-words", "avg,pg_typeof,current_setting", "comma-separated words that keep a statement on the heap wherever they appear in it")
	flag.StringVar(&cfg.provider, "mirror-plan-provider", "DuckDBScan", "Custom Scan provider the mirror-side plan must be; empty skips the check")
	flag.BoolVar(&cfg.logRoutes, "log-routes", false, "log every decision")
	flag.Parse()
	for _, w := range strings.Split(strings.ToLower(words), ",") {
		if w = strings.TrimSpace(w); w != "" {
			cfg.heapWords = append(cfg.heapWords, w)
		}
	}
	if !identRE.MatchString(cfg.mirrorSchema) || !identRE.MatchString(cfg.heapSchema) {
		fmt.Fprintln(os.Stderr, "qs-router: schema names must be plain lower-case identifiers")
		os.Exit(2)
	}
	srv := &server{cfg: cfg, cache: newDecisionCache(ttl, 20000), cats: &catalogCache{ttl: 30 * time.Second, m: map[string]catalogEntry{}}}
	src, err := newLSNSource(lsnSpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "qs-router:", err)
		os.Exit(2)
	}
	if src != nil {
		srv.fresh = &freshness{src: src, maxBytes: maxLag, hold: time.Second}
	} else {
		log.Printf("no -mirror-lsn: the mirror is used however far behind it is")
	}
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("qs-router on %s -> %s (heap %s, mirror %s, rows>%.0f or cost>%.0f, ttl %s)",
		cfg.listen, cfg.backend, cfg.heapSchema, cfg.mirrorSchema, cfg.th.Rows, cfg.th.Cost, ttl)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go srv.handle(c)
	}
}
