# LSN comparison, shared.
#
# PostgreSQL prints an LSN as two hex halves separated by a slash, so a string
# comparison is wrong in both directions: "1/9" sorts after "1/10", and
# "2/5" sorts before "10/5". Every script that waits for the mirror to catch up
# needs this, and it was written inline in one of them.

# lsn_ge <a> <b> — succeeds when a >= b.
lsn_ge() {
  python3 -c "
import sys
def p(l):
    a, b = l.split('/'); return (int(a, 16) << 32) | int(b, 16)
sys.exit(0 if p('$1') >= p('$2') else 1)" 2>/dev/null
}
