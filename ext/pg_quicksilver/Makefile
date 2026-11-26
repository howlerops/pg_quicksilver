# pg_quicksilver: routes read queries between heap tables and their mirrors.
#
#   make && make install
#   validate/setup.sh          # the cluster the tests need: a real mirror, pg_duckdb
#   make installcheck          # pg_regress against that cluster
#
# The regression tests read a mirror built by the sidecar, so they run against
# validate/setup.sh's database rather than a scratch one.
MODULES = pg_quicksilver
EXTENSION = pg_quicksilver
DATA = pg_quicksilver--0.1.sql
PGFILEDESC = "pg_quicksilver - fail-closed heap/mirror read routing"

REGRESS = routing failclosed permissions prepared transactions invalidation shadow freshness robustness
REGRESS_OPTS = --use-existing --host=/tmp --port=$(or $(PPORT),5541) --user=postgres

PG_CONFIG ?= /usr/lib/postgresql/17/bin/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# pgxs would otherwise test in contrib_regression
override CONTRIB_TESTDB = app
