package ducklake

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// reader is testdata/reader.py: DuckDB, in another process, attached
// read-only to the catalog under test. It is the real reader, and the tests
// that need it are skipped where python3 or duckdb is not installed.
type reader struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
}

type reply struct {
	Version    string            `json:"version"`
	Rows       [][]*string       `json:"rows"`
	Error      string            `json:"error"`
	Queries    int               `json:"queries"`
	RawAnswers []json.RawMessage `json:"answers"`
	Errors     []string          `json:"errors"`
}

func startReader(t testing.TB, catalog string) *reader {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed; the DuckDB conformance tests need it")
	}
	script, _ := filepath.Abs(filepath.Join("testdata", "reader.py"))
	cmd := exec.Command(py, script)
	cmd.Stderr = os.Stderr
	in, _ := cmd.StdinPipe()
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start python3: %v", err)
	}
	r := &reader{cmd: cmd, in: in, out: bufio.NewReaderSize(out, 1<<20)}
	t.Cleanup(func() {
		in.Close()
		_ = cmd.Wait()
	})
	rep, err := r.call(map[string]any{"attach": catalog})
	if err != nil {
		t.Skipf("python3 has no usable duckdb module: %v", err)
	}
	if rep.Error != "" {
		t.Skipf("DuckDB cannot attach a DuckLake here (extensions not installable?): %s", rep.Error)
	}
	t.Logf("reader: python duckdb %s", rep.Version)
	return r
}

func (r *reader) call(req map[string]any) (*reply, error) {
	b, _ := json.Marshal(req)
	if _, err := r.in.Write(append(b, '\n')); err != nil {
		return nil, err
	}
	line, err := r.out.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("reader exited: %w", err)
	}
	var rep reply
	if err := json.Unmarshal(line, &rep); err != nil {
		return nil, fmt.Errorf("reader said %q: %w", line, err)
	}
	return &rep, nil
}

// query runs sql and returns the rows as text ("<nil>" for NULL), or the
// reader's error.
func (r *reader) query(fresh bool, sql string) ([][]string, error) {
	rep, err := r.call(map[string]any{"sql": sql, "fresh": fresh})
	if err != nil {
		return nil, err
	}
	if rep.Error != "" {
		return nil, fmt.Errorf("%s", rep.Error)
	}
	return textRows(rep.Rows), nil
}

func textRows(in [][]*string) [][]string {
	out := make([][]string, len(in))
	for i, row := range in {
		for _, v := range row {
			if v == nil {
				out[i] = append(out[i], "<nil>")
			} else {
				out[i] = append(out[i], *v)
			}
		}
	}
	return out
}

func flat(rows [][]string) string {
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = strings.Join(r, ",")
	}
	return strings.Join(parts, ";")
}
