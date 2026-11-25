package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Every statement the router runs for itself goes through the extended
// protocol under this one name, never through a simple Query: a simple Query
// destroys the client's unnamed prepared statement and portal, and a client
// that Parses in one round trip and Binds in the next would find it gone.
const internalName = "qs_router_internal"

type prepared struct {
	sql  string
	oids []uint32
	info stmtInfo
}

// stmtReq is one statement about to run: a simple Query, or one Bind.
type stmtReq struct {
	info   *stmtInfo
	sql    string
	oids   []uint32
	fmts   []int16
	params [][]byte
	key    string // cache key its decision was stored under
}

type session struct {
	srv    *server
	cl, be *pipe
	db     string

	tx byte // last ReadyForQuery status: I, T or E

	mode   string // auto, heap, mirror
	scsOff bool   // standard_conforming_strings = off: the lexer is not safe

	stmts   map[string]*prepared
	pending []pendingParse

	schemasKey string // role:search_path the last probe ran under, "" if unknown
	last       string // what SHOW qs.last_route answers

	flush flushState
}

type pendingParse struct {
	name string
	p    *prepared
}

type iop struct {
	sql      string
	oids     []uint32
	fmts     []int16
	params   [][]byte
	describe bool // Parse and Describe only
}

type ires struct {
	rows  [][]string
	types []uint32 // from a Describe; nil if none came back
	desc  bool
	err   *pgproto3.ErrorResponse
}

// internal runs the router's own statements. Each batch is one Sync, so an
// error in one batch does not skip the next; all of them go out in one write.
func (s *session) internal(batches ...[]iop) ([]ires, error) {
	var out []byte
	for _, b := range batches {
		out, _ = (&pgproto3.Close{ObjectType: 'S', Name: internalName}).Encode(out)
		for _, op := range b {
			out, _ = (&pgproto3.Parse{Name: internalName, Query: op.sql, ParameterOIDs: op.oids}).Encode(out)
			if op.describe {
				out, _ = (&pgproto3.Describe{ObjectType: 'S', Name: internalName}).Encode(out)
			} else {
				var err error
				out, err = (&pgproto3.Bind{DestinationPortal: internalName, PreparedStatement: internalName,
					ParameterFormatCodes: op.fmts, Parameters: op.params}).Encode(out)
				if err != nil {
					return nil, err
				}
				out, _ = (&pgproto3.Execute{Portal: internalName}).Encode(out)
				// Closing the statement does not close its portal before the
				// Sync, and the next Bind in this batch reuses the name.
				out, _ = (&pgproto3.Close{ObjectType: 'P', Name: internalName}).Encode(out)
			}
			out, _ = (&pgproto3.Close{ObjectType: 'S', Name: internalName}).Encode(out)
		}
		out, _ = (&pgproto3.Sync{}).Encode(out)
	}
	if err := s.be.raw(out); err != nil {
		return nil, err
	}
	if err := s.be.flush(); err != nil {
		return nil, err
	}
	res := make([]ires, len(batches))
	for i := 0; i < len(batches); {
		typ, body, err := s.be.read()
		if err != nil {
			return nil, err
		}
		switch typ {
		case 'D':
			var d pgproto3.DataRow
			if err := d.Decode(body); err != nil {
				return nil, err
			}
			row := make([]string, len(d.Values))
			for j, v := range d.Values {
				row[j] = string(v)
			}
			res[i].rows = append(res[i].rows, row)
		case 'T':
			var d pgproto3.RowDescription
			if err := d.Decode(body); err != nil {
				return nil, err
			}
			res[i].desc = true
			res[i].types = make([]uint32, len(d.Fields))
			for j, f := range d.Fields {
				res[i].types[j] = f.DataTypeOID
			}
		case 'n':
			res[i].desc = true
		case 'E':
			if res[i].err == nil {
				e := new(pgproto3.ErrorResponse)
				if err := e.Decode(body); err != nil {
					return nil, err
				}
				res[i].err = e
				if e.Severity == "FATAL" || e.Severity == "PANIC" {
					return nil, fmt.Errorf("backend: %s", e.Message)
				}
			}
		case 'Z':
			s.tx = body[0]
			i++
		case 'A':
			// A NOTIFY that happened to arrive now still belongs to the client.
			if err := s.cl.write(typ, body); err != nil {
				return nil, err
			}
		case 'S':
			s.paramStatus(body, true)
		}
	}
	return res, nil
}

func (s *session) paramStatus(body []byte, internal bool) {
	name := cstring(body)
	val := ""
	if len(body) > len(name)+1 {
		val = cstring(body[len(name)+1:])
	}
	switch name {
	case "standard_conforming_strings":
		s.scsOff = val != "on"
	}
	// search_path is reported from PostgreSQL 18 on. The client must never see
	// the router's own change to it.
	if internal && name != "search_path" {
		_ = s.cl.write('S', body)
	}
}

func (s *session) mirrorRE() string { return "'^" + s.srv.cfg.mirrorSchema + "(, |$)'" }

// keySQL identifies what a statement's names resolve against: the role (for
// "$user") and search_path, without the mirror schema if the router put it there.
func (s *session) keySQL() string {
	return "current_user || ':' || regexp_replace(current_setting('search_path'), " + s.mirrorRE() + ", '')"
}

// preOp puts the mirror schema in front of search_path and reports, in the
// same row, who and where the session is and how far the server's WAL has got.
func (s *session) preOp() iop {
	m := s.srv.cfg.mirrorSchema
	base := "regexp_replace(current_setting('search_path'), " + s.mirrorRE() + ", '')"
	return iop{sql: "SELECT " + s.keySQL() + ", " +
		"(CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_lsn() END)::text, " +
		"set_config('search_path', '" + m + "' || CASE WHEN " + base + " = '' THEN '' ELSE ', ' || " + base + " END, false)"}
}

func (s *session) postOp() iop {
	return iop{sql: "SELECT set_config('search_path', regexp_replace(current_setting('search_path'), " + s.mirrorRE() + ", ''), false)"}
}

// enterMirror switches search_path for the session; leaveMirror switches it
// back. Only ever called outside a transaction block, so each is its own
// committed transaction and nothing the client does can roll one of them back.
func (s *session) enterMirror() (key string, lsn uint64, err error) {
	res, err := s.internal([]iop{s.preOp()})
	if err != nil {
		return "", 0, err
	}
	if res[0].err != nil || len(res[0].rows) != 1 {
		return "", 0, fmt.Errorf("could not switch search_path: %s", errText(res[0].err))
	}
	lsn, perr := ParseLSN(res[0].rows[0][1])
	if perr != nil {
		lsn = 0
	}
	return res[0].rows[0][0], lsn, nil
}

// leaveMirror failing is the one thing the router cannot recover from: the
// session would go on reading the mirror for statements routed to the heap.
// The caller closes the connection.
func (s *session) leaveMirror() error {
	res, err := s.internal([]iop{s.postOp()})
	if err != nil {
		return err
	}
	if res[0].err != nil {
		return fmt.Errorf("could not restore search_path: %s", errText(res[0].err))
	}
	return nil
}

func errText(e *pgproto3.ErrorResponse) string {
	if e == nil {
		return "no row"
	}
	return e.Code + " " + e.Message
}

func (s *session) catalog() (*Catalog, error) {
	if c := s.srv.cats.get(s.db); c != nil {
		return c, nil
	}
	cfg := s.srv.cfg
	sql := `SELECT 'm', c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = $1 AND c.relkind IN ('v','m')
   AND EXISTS (SELECT 1 FROM pg_catalog.pg_class h JOIN pg_catalog.pg_namespace hn ON hn.oid = h.relnamespace
                WHERE hn.nspname = $2 AND h.relname = c.relname AND h.relkind = 'r')
UNION ALL
SELECT 'u', i.relname FROM pg_catalog.pg_index x JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
  JOIN pg_catalog.pg_class t ON t.oid = x.indrelid JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
 WHERE n.nspname = $2 AND x.indisunique AND x.indpred IS NULL`
	res, err := s.internal([]iop{{sql: sql, params: [][]byte{[]byte(cfg.mirrorSchema), []byte(cfg.heapSchema)}}})
	if err != nil {
		return nil, err
	}
	if res[0].err != nil {
		return nil, fmt.Errorf("catalog: %s", errText(res[0].err))
	}
	c := &Catalog{HeapSchema: cfg.heapSchema, Mirrored: map[string]bool{}, Unique: map[string]bool{}}
	for _, r := range res[0].rows {
		if r[0] == "m" {
			c.Mirrored[r[1]] = true
		} else {
			c.Unique[r[1]] = true
		}
	}
	s.srv.cats.put(s.db, c)
	return c, nil
}

func paramsKey(r *stmtReq) string {
	if len(r.params) == 0 {
		return ""
	}
	var b strings.Builder
	for _, f := range r.fmts {
		fmt.Fprintf(&b, "\x00f%d", f)
	}
	for _, p := range r.params {
		if p == nil {
			b.WriteString("\x00n")
			continue
		}
		fmt.Fprintf(&b, "\x00%d:", len(p))
		b.Write(p)
	}
	return b.String()
}

var errCanceled = fmt.Errorf("canceled")

func sameTypes(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// decide answers for one statement, from the cache or by asking the server.
// Only called outside a transaction block: a failed EXPLAIN there costs
// nothing, where inside one it would abort the client's transaction.
func (s *session) decide(r *stmtReq) (Decision, bool, error) {
	cat, err := s.catalog()
	if err != nil {
		return Decision{}, false, err
	}
	cache := s.srv.cache
	prefix := func() string { return s.db + "\x00" + s.schemasKey + "\x00" + s.mode + "\x00" }
	if s.schemasKey != "" {
		if d, ok := cache.get(prefix() + "S" + r.info.Shape); ok {
			r.key = prefix() + "S" + r.info.Shape
			return d, true, nil
		}
		k := prefix() + "E" + r.info.Exact + paramsKey(r)
		if d, ok := cache.get(k); ok {
			r.key = k
			return d, true, nil
		}
	}

	// Ask. One round trip: who/where the session is, and the statement's plan
	// against the heap. The client's own parameter values go with it, so the
	// plan is the one these values would get.
	res, err := s.internal([]iop{
		{sql: "SELECT " + s.keySQL()},
		{sql: "EXPLAIN (VERBOSE, FORMAT JSON) " + r.sql, oids: r.oids, fmts: r.fmts, params: r.params},
	})
	if err != nil {
		return Decision{}, false, err
	}
	pr := res[0]
	if len(pr.rows) >= 1 {
		s.schemasKey = pr.rows[0][0]
	}
	store := func(d Decision) (Decision, bool, error) {
		if s.schemasKey != "" {
			if d.Stable {
				r.key = prefix() + "S" + r.info.Shape
			} else {
				r.key = prefix() + "E" + r.info.Exact + paramsKey(r)
			}
			cache.put(r.key, d)
		}
		return d, false, nil
	}
	if pr.err != nil || len(pr.rows) != 2 {
		if pr.err != nil && pr.err.Code == "57014" {
			return Decision{}, false, errCanceled
		}
		return store(Decision{Side: Heap, Reason: "PostgreSQL would not plan it: " + errText(pr.err)})
	}
	plan, err := ParsePlan([]byte(pr.rows[1][0]))
	if err != nil {
		return store(Decision{Side: Heap, Reason: "unreadable plan: " + err.Error()})
	}
	d := Decide(plan, cat, s.srv.cfg.th, s.mode == "mirror")
	if d.Side != Mirror {
		return store(d)
	}

	// The heap plan says mirror. Before believing it, plan the statement THERE:
	// DuckDB has to accept it, answer it itself, and return the same types.
	describe := iop{sql: r.sql, oids: r.oids, describe: true}
	res, err = s.internal([]iop{describe}, []iop{s.preOp()}, []iop{
		describe,
		{sql: "EXPLAIN (FORMAT JSON) " + r.sql, oids: r.oids, fmts: r.fmts, params: r.params},
	}, []iop{s.postOp()})
	if err != nil {
		return Decision{}, false, err
	}
	hr, vr := res[0], res[2]
	if res[1].err != nil || res[3].err != nil {
		return Decision{}, false, fmt.Errorf("could not switch search_path and back: %s / %s", errText(res[1].err), errText(res[3].err))
	}
	no := func(why string) (Decision, bool, error) {
		return store(Decision{Side: Heap, Reason: why})
	}
	switch {
	case vr.err != nil && vr.err.Code == "57014":
		return Decision{}, false, errCanceled
	case vr.err != nil:
		return no("the mirror cannot plan it: " + firstLine(vr.err.Message))
	case len(vr.rows) != 1:
		return no("the mirror returned no plan")
	case hr.err != nil || !hr.desc || !vr.desc:
		return no("could not compare what the two sides return")
	case !sameTypes(hr.types, vr.types):
		return no(fmt.Sprintf("the mirror returns other column types (%v, heap %v)", vr.types, hr.types))
	}
	if strings.Contains(strings.ToUpper(vr.rows[0][0]), "POSTGRES_SCAN") {
		return no("the mirror-side plan reads a heap table too")
	}
	mp, err := ParsePlan([]byte(vr.rows[0][0]))
	if err != nil {
		return no("unreadable mirror-side plan")
	}
	if want := s.srv.cfg.provider; want != "" && (mp.NodeType != "Custom Scan" || mp.CustomProvider != want) {
		return no("the mirror schema did not move it off the heap (" + mp.NodeType + ")")
	}
	if leak := HeapLeak(mp, cat); leak != "" {
		return no("with the mirror schema in front it still reads " + leak)
	}
	return store(d)
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

type routed struct {
	side   Side
	reason string
	cached bool
	refuse *pgproto3.ErrorResponse // answer the client with this instead of running anything
}

func heapBecause(why string) routed { return routed{side: Heap, reason: why} }

// route decides where a group of statements (one Query, or the Binds of one
// Sync) runs. It never switches search_path itself.
//
// A transaction block is always the heap's. The design asked for a
// transaction to stay on whichever side its first read chose, and that was
// built and then removed, because of what pg_duckdb 1.2.0 did under it, each
// reproduced in psql without the router:
//
//   - SAVEPOINT in a transaction that has run a DuckDB query aborts it
//     ("SAVEPOINT is not supported in DuckDB"), which is every nested
//     transaction pgx and psycopg open;
//   - after an EXPLAIN of a heap query, or a set_config(), or statements sent
//     through the extended protocol, the next DuckDB query in the same
//     transaction fails with "DuckDB execution is not supported inside
//     functions".
//
// The router's own probe is an EXPLAIN through the extended protocol, so it
// could not even look without breaking the transaction it was looking for.
func (s *session) route(reqs []*stmtReq) (routed, error) {
	switch {
	case s.tx == 'E':
		return heapBecause("aborted transaction"), nil
	case s.tx == 'T':
		return heapBecause("inside a transaction block"), nil
	case s.mode == "heap":
		return heapBecause("qs.route=heap"), nil
	case s.scsOff:
		return heapBecause("standard_conforming_strings is off"), nil
	case len(reqs) == 0:
		return heapBecause("nothing to execute"), nil
	}
	for _, r := range reqs {
		if ok, why := r.info.plainRead(s.srv.cfg.allowOrderBy, s.srv.cfg.heapWords); !ok {
			return heapBecause(why), nil
		}
	}
	out := routed{side: Mirror, cached: true}
	for _, r := range reqs {
		d, cached, err := s.decide(r)
		if err == errCanceled {
			return routed{refuse: pgError("57014", "canceling statement due to user request")}, nil
		}
		if err != nil {
			return routed{}, err
		}
		out.cached = out.cached && cached
		if d.Side == Mirror && out.reason == "" {
			out.reason = d.Reason
		}
		if d.Side != Mirror && out.side == Mirror {
			out.side, out.reason = Heap, d.Reason
		}
	}
	return out, nil
}

// run sends one client exchange to the side chosen for it and relays the
// answer. send writes the client's messages to the backend.
func (s *session) run(reqs []*stmtReq, infos []*stmtInfo, ext, replayable bool, send func() error) error {
	var rt routed
	entered := false
	for attempt := 0; ; attempt++ {
		var err error
		if rt, err = s.route(reqs); err != nil {
			return err
		}
		if rt.refuse != nil || rt.side != Mirror {
			break
		}
		key, lsn, err := s.enterMirror()
		if err != nil {
			return err
		}
		entered = true
		switch {
		case key != s.schemasKey && attempt == 0:
			// The decision was made for another role or search_path: the same
			// text may name other tables now. Undo, and decide again.
			if err := s.leaveMirror(); err != nil {
				return err
			}
			entered = false
			s.schemasKey = key
			continue
		case key != s.schemasKey:
			rt = heapBecause("search_path keeps changing under the router")
		case s.srv.fresh != nil:
			if ok, why := s.srv.fresh.check(context.Background(), lsn); !ok {
				rt = heapBecause(why)
			}
		}
		if rt.side != Mirror {
			if err := s.leaveMirror(); err != nil {
				return err
			}
			entered = false
		}
		break
	}

	if rt.refuse != nil {
		s.last = "refused: " + rt.refuse.Message
		s.pending = s.pending[:0]
		s.srv.logRoute(s.last, infos)
		if err := s.cl.send(rt.refuse, &pgproto3.ReadyForQuery{TxStatus: s.tx}); err != nil {
			return err
		}
		return s.cl.flush()
	}
	s.last = rt.side.String() + ": " + rt.reason
	if rt.cached {
		s.last += " [cached]"
	}
	s.srv.logRoute(s.last, infos)

	if err := send(); err != nil {
		return err
	}
	if err := s.be.flush(); err != nil {
		return err
	}
	failed, retry, err := s.relay(ext, entered && replayable)
	if err != nil {
		return err
	}
	if retry {
		// The mirror refused it before answering anything: a view whose files
		// a compaction has since replaced, a value DuckDB cannot cast. The
		// heap can still answer, and the client has seen nothing yet.
		if s.tx != 'I' {
			return fmt.Errorf("a statement sent to the mirror left a transaction open")
		}
		if err := s.leaveMirror(); err != nil {
			return err
		}
		entered = false
		why := "the mirror failed it: " + firstLine(failed.Message)
		for _, r := range reqs {
			if r.key != "" {
				s.srv.cache.put(r.key, Decision{Side: Heap, Reason: why})
			}
		}
		rt = heapBecause(why + "; answered by the heap instead")
		s.last = "heap: " + rt.reason
		s.srv.logRoute(s.last, infos)
		if err := send(); err != nil {
			return err
		}
		if err := s.be.flush(); err != nil {
			return err
		}
		if failed, _, err = s.relay(ext, false); err != nil {
			return err
		}
	}
	if entered {
		if s.tx != 'I' {
			return fmt.Errorf("a statement sent to the mirror left a transaction open")
		}
		if err := s.leaveMirror(); err != nil {
			return err
		}
	}
	if rt.side == Mirror && failed != nil && failed.Code != "57014" {
		// It planned on the mirror and then failed there. Do not send the next
		// one after it.
		for _, r := range reqs {
			if r.key != "" {
				s.srv.cache.put(r.key, Decision{Side: Heap, Reason: "the mirror failed it last time: " + firstLine(failed.Message)})
			}
		}
	}
	s.after(infos, failed != nil)
	return nil
}

// after updates what the router believes about the session from the
// statements that just ran.
func (s *session) after(infos []*stmtInfo, failed bool) {
	for _, in := range infos {
		if in.TouchesPrep {
			// PREPARE, DEALLOCATE and DISCARD share the protocol's statement
			// names. Forget them all; an unknown name goes to the heap.
			s.stmts = map[string]*prepared{}
		}
		if failed {
			continue
		}
		switch {
		case in.RouteReset:
			s.mode = "auto"
		case in.Route == "auto" || in.Route == "heap" || in.Route == "mirror":
			s.mode = in.Route
		case in.Route != "":
			s.mode = "heap" // a value the router does not know: the safe one
		}
	}
}

// Messages that can precede the first result of a statement. While only these
// have arrived, nothing the client could act on has been sent.
var prelude = map[byte]bool{'1': true, '2': true, '3': true, 't': true, 'T': true, 'n': true, 'N': true, 'S': true}

// relay forwards the backend's answer to one Sync (or one simple Query) and
// returns the first error in it.
//
// With hold set, the answer is kept back until its first result message. If an
// error comes before that, nothing is forwarded at all and retry is true: the
// statement failed on the mirror before the client saw anything of it, and the
// caller can still run it on the heap.
func (s *session) relay(ext, hold bool) (failed *pgproto3.ErrorResponse, retry bool, err error) {
	var held []byte
	for {
		typ, body, err := s.be.read()
		if err != nil {
			return nil, false, err
		}
		switch typ {
		case 'Z':
			s.tx = body[0]
			s.pending = s.pending[:0]
		case 'E':
			if failed == nil {
				failed = new(pgproto3.ErrorResponse)
				if err := failed.Decode(body); err != nil {
					return nil, false, err
				}
				if hold {
					retry = true
				}
			}
		case '1':
			if len(s.pending) > 0 {
				p := s.pending[0]
				s.pending = s.pending[1:]
				s.stmts[p.name] = p.p
			}
		case 'S':
			s.paramStatus(body, false)
		}
		if retry {
			if typ == 'Z' {
				return failed, true, nil
			}
			continue
		}
		if hold {
			if prelude[typ] {
				held = frame(held, typ, body)
				continue
			}
			hold = false
			if err := s.cl.raw(held); err != nil {
				return nil, false, err
			}
		}
		if err := s.cl.write(typ, body); err != nil {
			return nil, false, err
		}
		if typ == 'G' || typ == 'W' {
			if err := s.cl.flush(); err != nil {
				return nil, false, err
			}
			if err := s.copyIn(ext); err != nil {
				return nil, false, err
			}
			continue
		}
		if typ == 'Z' {
			return failed, false, s.cl.flush()
		}
		if s.be.r.Buffered() == 0 {
			if err := s.cl.flush(); err != nil {
				return nil, false, err
			}
		}
	}
}

// copyIn carries the client's COPY data to the backend. After CopyDone or
// CopyFail an extended-protocol client owes one more Sync (the one it sent with
// Execute was swallowed by copy-in mode), and the backend answers that one.
func (s *session) copyIn(ext bool) error {
	done := false
	for {
		typ, body, err := s.cl.read()
		if err != nil {
			return err
		}
		if err := s.be.write(typ, body); err != nil {
			return err
		}
		if typ == 'c' || typ == 'f' {
			done = true
		}
		if (done && (!ext || typ == 'S')) || typ == 'X' {
			return s.be.flush()
		}
		if s.cl.r.Buffered() == 0 {
			if err := s.be.flush(); err != nil {
				return err
			}
		}
	}
}

// simple handles a Query message.
func (s *session) simple(body []byte) error {
	sql := cstring(body)
	delete(s.stmts, "") // a simple Query destroys the unnamed statement
	info := analyse(sql, s.srv.cfg.heapSchema, s.srv.cfg.mirrorSchema)
	if info.ShowLast {
		return s.showLast()
	}
	msg := frame(nil, 'Q', body)
	var reqs []*stmtReq
	if !info.Empty {
		reqs = []*stmtReq{{info: &info, sql: sql}}
	}
	return s.run(reqs, []*stmtInfo{&info}, false, true, func() error { return s.be.raw(msg) })
}

func (s *session) showLast() error {
	if err := s.cl.send(
		&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("qs.last_route"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}},
		&pgproto3.DataRow{Values: [][]byte{[]byte(s.last)}},
		&pgproto3.CommandComplete{CommandTag: []byte("SHOW")},
		&pgproto3.ReadyForQuery{TxStatus: s.tx},
	); err != nil {
		return err
	}
	return s.cl.flush()
}

// batch is the extended-protocol messages collected up to a Sync.
type batch struct {
	wire   []byte
	local  map[string]*prepared
	parses []pendingParse
	closes []string
	reqs   []*stmtReq
	infos  []*stmtInfo
	heap   string // a reason the whole batch must stay on the heap
}

func (b *batch) reset() {
	*b = batch{wire: b.wire[:0]}
}

// add records one extended-protocol message. body is copied.
func (s *session) add(b *batch, typ byte, body []byte) error {
	start := len(b.wire)
	b.wire = frame(b.wire, typ, body)
	own := b.wire[start+5:]
	switch typ {
	case 'P':
		var p pgproto3.Parse
		if err := p.Decode(own); err != nil {
			return err
		}
		pr := &prepared{sql: p.Query, oids: append([]uint32(nil), p.ParameterOIDs...)}
		pr.info = analyse(p.Query, s.srv.cfg.heapSchema, s.srv.cfg.mirrorSchema)
		if b.local == nil {
			b.local = map[string]*prepared{}
		}
		b.local[p.Name] = pr
		b.parses = append(b.parses, pendingParse{p.Name, pr})
	case 'B':
		var m pgproto3.Bind
		if err := m.Decode(own); err != nil {
			return err
		}
		pr := b.local[m.PreparedStatement]
		if pr == nil {
			pr = s.stmts[m.PreparedStatement]
		}
		if pr == nil {
			if b.heap == "" {
				b.heap = "a prepared statement the router did not see prepared"
			}
			return nil
		}
		b.reqs = append(b.reqs, &stmtReq{info: &pr.info, sql: pr.sql, oids: pr.oids,
			fmts: append([]int16(nil), m.ParameterFormatCodes...), params: m.Parameters})
		b.infos = append(b.infos, &pr.info)
	case 'C':
		if len(own) > 0 && own[0] == 'S' {
			b.closes = append(b.closes, cstring(own[1:]))
		}
	}
	return nil
}

func (s *session) runBatch(b *batch) error {
	for _, n := range b.closes {
		delete(s.stmts, n)
	}
	s.pending = append(s.pending[:0], b.parses...)
	reqs := b.reqs
	if b.heap != "" {
		// Unknown statement: route() must not see a partial picture.
		s.last = "heap: " + b.heap
		s.srv.logRoute(s.last, b.infos)
		if err := s.be.raw(b.wire); err != nil {
			return err
		}
		if err := s.be.flush(); err != nil {
			return err
		}
		failed, _, err := s.relay(true, false)
		if err != nil {
			return err
		}
		s.after(b.infos, failed != nil)
		return nil
	}
	// A named statement created by this batch outlives the batch's failure, so
	// sending the batch twice would fail the second time on the name.
	replayable := true
	for _, p := range b.parses {
		if p.name != "" {
			replayable = false
		}
	}
	return s.run(reqs, b.infos, true, replayable, func() error { return s.be.raw(b.wire) })
}

// flushState is the pass-through the session falls into when a client sends
// Flush: it wants answers before its Sync, so the router can no longer treat
// the batch as one unit to place. Everything goes to the heap, unread, until
// the backend has answered every Sync that was forwarded.
type flushState struct {
	active bool
	mu     sync.Mutex
	syncs  int
	readys int
	dirty  bool // something was forwarded after the last Sync
	done   bool
	err    chan error
}

func (s *session) startFlush(b *batch) error {
	f := &s.flush
	f.active, f.syncs, f.readys, f.dirty, f.done = true, 0, 0, true, false
	f.err = make(chan error, 1)
	// Statements parsed from here on are not tracked, and names closed or
	// re-parsed are forgotten: an unknown name is routed to the heap.
	for _, n := range b.closes {
		delete(s.stmts, n)
	}
	for _, p := range b.parses {
		delete(s.stmts, p.name)
	}
	s.pending = s.pending[:0]
	if err := s.be.raw(b.wire); err != nil {
		return err
	}
	if err := s.be.flush(); err != nil {
		return err
	}
	s.last = "heap: Flush before Sync (pass-through)"
	s.srv.logRoute(s.last, b.infos)
	go func() {
		for {
			typ, body, err := s.be.read()
			if err != nil {
				f.err <- err
				return
			}
			if err := s.cl.write(typ, body); err != nil {
				f.err <- err
				return
			}
			if typ == 'Z' {
				f.mu.Lock()
				f.readys++
				s.tx = body[0]
				fin := f.readys >= f.syncs && !f.dirty
				if fin {
					f.done = true
				}
				f.mu.Unlock()
				if err := s.cl.flush(); err != nil {
					f.err <- err
					return
				}
				if fin {
					f.err <- nil
					return
				}
				continue
			}
			if s.be.r.Buffered() == 0 {
				if err := s.cl.flush(); err != nil {
					f.err <- err
					return
				}
			}
		}
	}()
	return nil
}

// forwardFlush passes one client message through while in flush mode. It
// reports whether the mode has ended, in which case the message was NOT
// forwarded and the caller handles it normally.
func (s *session) forwardFlush(typ byte, body []byte) (ended bool, err error) {
	f := &s.flush
	f.mu.Lock()
	if f.done {
		f.mu.Unlock()
		f.active = false
		if err := <-f.err; err != nil {
			return true, err
		}
		s.stmts = map[string]*prepared{}
		return true, nil
	}
	if typ == 'S' {
		f.syncs++
		f.dirty = false
	} else if typ != 'H' {
		// A Flush asks for nothing new. libpq sends one right after the Sync
		// that ends a pipeline, and counting it left the session in
		// pass-through for good.
		f.dirty = true
	}
	f.mu.Unlock()
	if typ == 'P' || typ == 'C' {
		s.stmts = map[string]*prepared{}
	}
	if err := s.be.write(typ, body); err != nil {
		return false, err
	}
	if s.cl.r.Buffered() == 0 || typ == 'S' || typ == 'H' {
		if err := s.be.flush(); err != nil {
			return false, err
		}
	}
	return false, nil
}

// serve is the main loop, after authentication.
func (s *session) serve() error {
	var b batch
	for {
		typ, body, err := s.cl.read()
		if err != nil {
			return err
		}
		if s.flush.active {
			ended, err := s.forwardFlush(typ, body)
			if err != nil {
				return err
			}
			if !ended {
				if typ == 'X' {
					return nil
				}
				continue
			}
		}
		switch typ {
		case 'Q':
			if len(b.wire) > 0 {
				// Extended messages with no Sync, then a Query: not something a
				// driver does. Hand both over in order and let the server judge.
				if err := s.be.raw(b.wire); err != nil {
					return err
				}
				b.reset()
			}
			if err := s.simple(body); err != nil {
				return err
			}
		case 'X':
			_ = s.be.write(typ, body)
			_ = s.be.flush()
			return nil
		case 'P', 'B', 'D', 'E', 'C':
			if err := s.add(&b, typ, body); err != nil {
				return err
			}
		case 'S':
			b.wire = frame(b.wire, typ, body)
			if err := s.runBatch(&b); err != nil {
				return err
			}
			b.reset()
		case 'H':
			if len(b.wire) == 0 {
				// Nothing is waiting to be flushed out of the router.
				if err := s.be.write(typ, body); err != nil {
					return err
				}
				if err := s.be.flush(); err != nil {
					return err
				}
				continue
			}
			b.wire = frame(b.wire, typ, body)
			if err := s.startFlush(&b); err != nil {
				return err
			}
			b.reset()
		case 'F':
			// Function call (fastpath): deprecated, answered with one
			// ReadyForQuery. Heap.
			delete(s.stmts, "")
			if err := s.be.write(typ, body); err != nil {
				return err
			}
			if err := s.be.flush(); err != nil {
				return err
			}
			if _, _, err := s.relay(false, false); err != nil {
				return err
			}
		default:
			// CopyData/CopyDone/CopyFail outside a COPY, or something unknown:
			// the server ignores or rejects it, so it should be the one to.
			if err := s.be.write(typ, body); err != nil {
				return err
			}
			if err := s.be.flush(); err != nil {
				return err
			}
		}
	}
}

func be32(b []byte) uint32 {
	if len(b) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
