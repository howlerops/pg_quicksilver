package main

import (
	"strings"
)

// The router has no SQL parser. What it needs from a statement's text is small
// and all of it is used to say NO: is this one statement, does it start like a
// read, does it name a schema itself, does it carry an ORDER BY. Whether it IS a
// read is PostgreSQL's answer (the plan), never this file's.
//
// So the lexer only has to be right about where literals, comments and quoted
// identifiers begin and end. Anything it cannot close is "bad" and goes to the
// heap untouched.

type tokKind uint8

const (
	tWord   tokKind = iota // keyword or unquoted identifier, lowercased
	tQIdent                // "Quoted", kept verbatim including the quotes
	tString                // any string literal, kept verbatim
	tNumber
	tParam // $1
	tPunct
)

type token struct {
	kind tokKind
	text string
}

// stmtInfo is everything the router knows about a statement from its text.
type stmtInfo struct {
	// Exact is the statement with comments removed and whitespace collapsed:
	// the cache key for a statement whose plan depends on its constants.
	Exact string
	// Shape is Exact with every literal replaced by '?': the cache key for a
	// statement whose plan does not.
	Shape string
	First string // first keyword, lowercased; leading parentheses skipped
	Bad   bool   // unterminated literal or comment
	Multi bool   // more than one statement
	Empty bool

	Qualified string // a schema this statement names itself, "" if none of ours
	HasInto   bool
	Divides   bool // a '/' operator somewhere
	Words     map[string]bool
	OrderBy   bool

	TouchesPrep bool // PREPARE / DEALLOCATE / DISCARD somewhere in it

	// Route is the value of `SET qs.route = ...`; RouteReset is RESET qs.route,
	// RESET ALL or DISCARD ALL. ShowLast is `SHOW qs.last_route`.
	Route      string
	RouteReset bool
	ShowLast   bool
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentPart(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$' }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// lex splits SQL into tokens. ok is false when something was left open.
func lex(sql string) (toks []token, ok bool) {
	i, n := 0, len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth, j := 1, i+2
			for j < n && depth > 0 {
				switch {
				case sql[j] == '/' && j+1 < n && sql[j+1] == '*':
					depth++
					j += 2
				case sql[j] == '*' && j+1 < n && sql[j+1] == '/':
					depth--
					j += 2
				default:
					j++
				}
			}
			if depth > 0 {
				return toks, false
			}
			i = j
		case c == '\'':
			// E'...' honours backslashes; the word before the quote says so.
			esc := false
			if len(toks) > 0 && toks[len(toks)-1].kind == tWord && toks[len(toks)-1].text == "e" &&
				i > 0 && (sql[i-1] == 'e' || sql[i-1] == 'E') {
				esc = true
			}
			j := i + 1
			closed := false
			for j < n {
				if esc && sql[j] == '\\' {
					j += 2
					continue
				}
				if sql[j] == '\'' {
					if j+1 < n && sql[j+1] == '\'' {
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				j++
			}
			if !closed {
				return toks, false
			}
			toks = append(toks, token{tString, sql[i:j]})
			i = j
		case c == '"':
			j := i + 1
			closed := false
			for j < n {
				if sql[j] == '"' {
					if j+1 < n && sql[j+1] == '"' {
						j += 2
						continue
					}
					closed = true
					j++
					break
				}
				j++
			}
			if !closed {
				return toks, false
			}
			toks = append(toks, token{tQIdent, sql[i:j]})
			i = j
		case c == '$':
			if i+1 < n && isDigit(sql[i+1]) {
				j := i + 1
				for j < n && isDigit(sql[j]) {
					j++
				}
				toks = append(toks, token{tParam, sql[i:j]})
				i = j
				break
			}
			// $tag$ ... $tag$
			j := i + 1
			for j < n && (isIdentStart(sql[j]) || isDigit(sql[j])) {
				j++
			}
			if j >= n || sql[j] != '$' {
				toks = append(toks, token{tPunct, "$"})
				i++
				break
			}
			tag := sql[i : j+1]
			end := strings.Index(sql[j+1:], tag)
			if end < 0 {
				return toks, false
			}
			stop := j + 1 + end + len(tag)
			toks = append(toks, token{tString, sql[i:stop]})
			i = stop
		case isDigit(c) || (c == '.' && i+1 < n && isDigit(sql[i+1])):
			j := i + 1
			for j < n && (isDigit(sql[j]) || sql[j] == '.' || sql[j] == '_' || isIdentStart(sql[j]) ||
				((sql[j] == '+' || sql[j] == '-') && (sql[j-1] == 'e' || sql[j-1] == 'E'))) {
				j++
			}
			toks = append(toks, token{tNumber, sql[i:j]})
			i = j
		case isIdentStart(c):
			j := i + 1
			for j < n && isIdentPart(sql[j]) {
				j++
			}
			toks = append(toks, token{tWord, strings.ToLower(sql[i:j])})
			i = j
		default:
			toks = append(toks, token{tPunct, sql[i : i+1]})
			i++
		}
	}
	return toks, true
}

func unquoteIdent(t token) string {
	if t.kind == tQIdent {
		return strings.ReplaceAll(t.text[1:len(t.text)-1], `""`, `"`)
	}
	return t.text
}

func isName(t token) bool { return t.kind == tWord || t.kind == tQIdent }

// analyse reads a statement's text. heapSchema and mirrorSchema are the two
// schemas a statement must not name itself if the router is to choose for it.
func analyse(sql, heapSchema, mirrorSchema string) stmtInfo {
	toks, ok := lex(sql)
	var in stmtInfo
	in.Bad = !ok
	// A trailing semicolon is not a second statement.
	for len(toks) > 0 && toks[len(toks)-1].kind == tPunct && toks[len(toks)-1].text == ";" {
		toks = toks[:len(toks)-1]
	}
	if len(toks) == 0 {
		in.Empty = true
		return in
	}
	var exact, shape strings.Builder
	for i, t := range toks {
		if i > 0 {
			exact.WriteByte(' ')
			shape.WriteByte(' ')
		}
		exact.WriteString(t.text)
		if t.kind == tString || t.kind == tNumber {
			shape.WriteByte('?')
		} else {
			shape.WriteString(t.text)
		}
		if t.kind == tPunct && t.text == ";" {
			in.Multi = true
		}
	}
	in.Exact, in.Shape = exact.String(), shape.String()

	first := 0
	for first < len(toks) && toks[first].kind == tPunct && toks[first].text == "(" {
		first++
	}
	if first < len(toks) && toks[first].kind == tWord {
		in.First = toks[first].text
	}

	word := func(i int, w string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == tWord && toks[i].text == w
	}
	punct := func(i int, p string) bool {
		return i >= 0 && i < len(toks) && toks[i].kind == tPunct && toks[i].text == p
	}

	for i, t := range toks {
		if isName(t) && punct(i+1, ".") {
			if s := unquoteIdent(t); s == heapSchema || s == mirrorSchema {
				in.Qualified = s
			}
		}
		if t.kind == tPunct && t.text == "/" {
			in.Divides = true
		}
		if t.kind != tWord {
			continue
		}
		if in.Words == nil {
			in.Words = map[string]bool{}
		}
		in.Words[t.text] = true
		switch t.text {
		case "into":
			in.HasInto = true
		case "order":
			if word(i+1, "by") {
				in.OrderBy = true
			}
		case "prepare", "deallocate", "discard":
			in.TouchesPrep = true
		}
	}

	// SET [SESSION|LOCAL] qs.route {=|TO} value
	if in.First == "set" && !in.Multi {
		i := 1
		if word(i, "session") || word(i, "local") {
			i++
		}
		if word(i, "qs") && punct(i+1, ".") && word(i+2, "route") && (punct(i+3, "=") || word(i+3, "to")) && i+5 == len(toks) {
			v := toks[i+4]
			switch v.kind {
			case tString:
				if len(v.text) >= 2 && v.text[0] == '\'' {
					in.Route = strings.ToLower(v.text[1 : len(v.text)-1])
				} else {
					in.Route = "?"
				}
			case tWord:
				in.Route = v.text
				if v.text == "default" {
					in.Route, in.RouteReset = "", true
				}
			default:
				in.Route = "?"
			}
		}
	}
	if in.First == "reset" && (word(1, "all") || (word(1, "qs") && punct(2, ".") && word(3, "route"))) {
		in.RouteReset = true
	}
	if in.First == "discard" && word(1, "all") {
		in.RouteReset = true
	}
	if in.First == "show" && len(toks) == 4 && word(1, "qs") && punct(2, ".") && word(3, "last_route") {
		in.ShowLast = true
	}
	return in
}

// plainRead says whether the text even looks like something to plan, and why
// not. A yes here is only permission to ask PostgreSQL.
func (in *stmtInfo) plainRead(allowOrderBy bool, heapWords []string) (bool, string) {
	switch {
	case in.Empty:
		return false, "empty statement"
	case in.Bad:
		return false, "text the router could not tokenise"
	case in.Multi:
		return false, "more than one statement"
	}
	switch in.First {
	case "select", "with", "table", "values":
	default:
		return false, "not a plain read (" + strings.ToUpper(in.First) + ")"
	}
	switch {
	case in.HasInto:
		return false, "contains INTO, which may create a table"
	case in.Qualified != "":
		return false, "names schema " + in.Qualified + " itself"
	case in.Divides:
		// Measured: SELECT 7/2 is 3 on the heap and 3.5 through pg_duckdb.
		return false, "division, which DuckDB does not truncate for integers"
	case in.OrderBy && !allowOrderBy:
		return false, "ORDER BY, which DuckDB sorts NULLs and text differently for"
	}
	for _, w := range heapWords {
		if in.Words[w] {
			return false, "uses " + w + ", which DuckDB answers differently"
		}
	}
	return true, ""
}
