/*
 * pg_quicksilver: routes read queries between heap tables and their columnar
 * mirrors, inside PostgreSQL, fail-closed.
 *
 * The heap is always correct. A statement is answered from the mirror only
 * when every construct in its typed query tree is on a verified allowlist,
 * every relation it reads has a usable, fresh mirror, the heap plan is
 * expensive enough to be worth it, and pg_duckdb accepts it and returns the
 * heap's column types. Anything else stays on the heap.
 *
 * The design and its measurements are in docs/40-the-router-moves-inside.md.
 *
 * Copyright (c) the pg_quicksilver authors. Released under the PostgreSQL License.
 */
#include "postgres.h"

#include <float.h>
#include <limits.h>
#include <sys/stat.h>

#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/dependency.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/queryjumble.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "port/atomics.h"
#include "tcop/cmdtag.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/tuplestore.h"
#include "utils/inval.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/pg_locale.h"
#include "utils/pg_lsn.h"
#include "utils/portal.h"
#include "utils/wait_event.h"
#include "utils/plancache.h"
#include "utils/regproc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

#define QS_SCHEMA			"quicksilver"
#define QS_EXTENSION		"pg_quicksilver"
#define QS_SCAN_NAME		"QuicksilverRoute"
#define QS_DUCKDB_SCAN_NAME	"DuckDBScan"
#define QS_GENERIC_PENALTY	1.0e10
#define QS_DETAIL_LEN		200

/* ---------------------------------------------------------------- GUCs */

typedef enum
{
	QS_ROUTE_OFF,
	QS_ROUTE_SHADOW,
	QS_ROUTE_AUTO,
	QS_ROUTE_HEAP,
	QS_ROUTE_MIRROR,
} QsRouteMode;

static const struct config_enum_entry qs_route_options[] = {
	{"off", QS_ROUTE_OFF, false},
	{"shadow", QS_ROUTE_SHADOW, false},
	{"auto", QS_ROUTE_AUTO, false},
	{"heap", QS_ROUTE_HEAP, false},
	{"mirror", QS_ROUTE_MIRROR, false},
	{NULL, 0, false}
};

static int	qs_route = QS_ROUTE_OFF;
static double qs_min_cost = 10000.0;
static double qs_min_rows = 50000.0;
static int	qs_max_lag_kb = 0;
static int	qs_fresh_wait_ms = 0;
static char *qs_min_applied_lsn_str = NULL;
static XLogRecPtr qs_min_applied_lsn = InvalidXLogRecPtr;
static bool qs_allow_unsafe = false;
static bool qs_route_in_xact = true;
static int	qs_wait_max_ms = 1000;
static double qs_wait_fraction = 0.5;
static double qs_cost_unit_ms = 0.002;
static bool qs_prefer_custom = true;
static bool qs_fallback = true;
static bool qs_log_decisions = false;
static bool qs_tolerate_lost_errors = false;
static int	qs_negative_ttl = 60;
static int	qs_stats_max = 2000;

/* ---------------------------------------------------------------- reasons */

typedef enum
{
	QR_ROUTED,
	QR_SHADOW,
	QR_MODE_HEAP,
	QR_NOT_PLAIN_SELECT,
	QR_NESTED,
	QR_XACT_BLOCK,
	QR_ISOLATION,
	QR_CURSOR,
	QR_NOT_MIRRORED,
	QR_CHEAP,
	QR_UNSAFE,
	QR_GENERIC_PLAN,
	QR_STALE,
	QR_NO_PRIVILEGE,
	QR_MIRROR_PLAN_FAILED,
	QR_OUTPUT_MISMATCH,
	QR_REGISTRY_CHANGED,
	QR_FALLBACK,
	QR_UNCONFINED,
	QR_NREASONS
} QsReason;

static const char *const qs_reason_names[QR_NREASONS] = {
	"routed",
	"shadow: would route",
	"quicksilver.route = heap",
	"not a plain SELECT",
	"inside a function or nested statement",
	"inside a transaction block",
	"transaction isolation is not READ COMMITTED",
	"scrollable cursor",
	"reads a relation with no usable mirror",
	"cheap on the heap",
	"construct not on the allowlist",
	"generic plan: parameter values unknown",
	"mirror is not fresh enough",
	"no privilege on the heap table",
	"the mirror could not plan it",
	"the mirror returns a different column type",
	"registry changed while planning",
	"mirror failed at execution; answered from the heap",
	"the role is not a superuser (see docs/40)",
};

/* ---------------------------------------------------------------- shared memory */

typedef struct QsStatKey
{
	Oid			dbid;
	uint64		queryid;
} QsStatKey;

typedef struct QsStatEntry
{
	QsStatKey	key;
	pg_atomic_uint64 n_mirror;
	pg_atomic_uint64 n_heap;
	pg_atomic_uint64 n_fallback;
	int32		last_reason;
	bool		last_mirror;
	char		last_detail[QS_DETAIL_LEN];
} QsStatEntry;

typedef struct QsLsnKey
{
	Oid			dbid;
	Oid			relid;
} QsLsnKey;

typedef struct QsLsnEntry
{
	QsLsnKey	key;
	XLogRecPtr	lsn;
	TimestampTz at;
} QsLsnEntry;

typedef struct QsShared
{
	LWLock	   *lock;
	pg_atomic_uint64 reasons[QR_NREASONS];
	pg_atomic_uint64 dropped;
	ConditionVariable reported;	/* an applied LSN was reported: wakes statements waiting for their mirror */
} QsShared;

static QsShared *qs_shared = NULL;
static HTAB *qs_stat_hash = NULL;
static HTAB *qs_lsn_hash = NULL;

#define QS_LSN_MAX 1024

/* ---------------------------------------------------------------- backend caches */

typedef enum
{
	QA_TYPE,
	QA_OPER,
	QA_FUNC,
	QA_AGG,
	QA_WINDOW,
	QA_CAST,
} QsAllowKind;

typedef enum
{
	QC_NONE,					/* valid only when no collation is involved */
	QC_ANY,						/* insensitive to the collation */
	QC_DETERMINISTIC,			/* equality: any deterministic collation */
	QC_C,						/* ordering: only C/POSIX */
} QsCollRule;

typedef struct QsAllowKey
{
	int32		kind;
	Oid			oid;
	Oid			oid2;
} QsAllowKey;

typedef struct QsAllowEntry
{
	QsAllowKey	key;
	int32		coll;
	bool		need_typmod;	/* type: its scale must be known statically */
	bool		finite;			/* type: a column must be asserted free of +-infinity */
	bool		io;				/* cast: admitted as an I/O conversion too */
	bool		cmp;			/* operator: a comparison; its result does not depend on its operands' scale */
	bool		cmp_only;		/* function: only as the direct operand of a comparison */
	bool		errors;			/* PostgreSQL can raise an error from it that the mirror may never evaluate */
} QsAllowEntry;

typedef struct QsNameEntry
{
	char		name[64];		/* "node:BoolExpr", "feature:order_by" */
} QsNameEntry;

typedef struct QsMirror
{
	Oid			heap;			/* hash key */
	Oid			mirror;
	bool		usable;
	bool		rls;
	char	   *reason;
	char	   *heapname;
	int64		max_lag;		/* bytes; -1: the GUC */
	XLogRecPtr	table_lsn;
	List	   *finite_cols;	/* names asserted to hold no +-infinity */
	int			natts;
	bool	   *col_ok;			/* [attno - 1] */
	char	  **col_why;
	Query	   *subquery;		/* SELECT <heap columns> FROM mirror, rewritten */
} QsMirror;

typedef struct QsWatch
{
	Oid			relid;
} QsWatch;

typedef struct QsDecisionCache
{
	uint64		queryid;		/* hash key */
	uint64		generation;
	bool		negative;
	bool		no_params;
	int32		reason;
	TimestampTz expires;		/* 0: with the generation */
	char		detail[QS_DETAIL_LEN];
} QsDecisionCache;

static MemoryContext qs_cache_cxt = NULL;
static bool qs_cache_valid = false;
static bool qs_installed = false;	/* the extension exists in this database */
static Oid	qs_mirrors_relid = InvalidOid;
static Oid	qs_allowlist_relid = InvalidOid;
static Oid	qs_lakes_relid = InvalidOid;

/*
 * A DuckLake the mirror relations read (quicksilver.lakes). pg_duckdb's DuckDB
 * lives in the backend, so a lake has to be attached once in every session
 * before a mirror relation over it can be planned. The extension does that
 * itself, the first time it is about to ask the mirror, and only if the
 * catalog file is on this node: there is no login trigger to lock anybody out,
 * and a session that was open before the lake existed is routed all the same.
 */
typedef struct QsLake
{
	char	   *alias;
	char	   *catalog;
} QsLake;
static List *qs_lakes = NIL;	/* QsLake, in qs_cache_cxt */
static bool qs_lakes_attached = false;	/* in this backend, for this load of the registry */
static TimestampTz qs_attach_retry_at = 0;

/*
 * pg_duckdb's own state, as far as this extension has to know it.
 *
 * qs_duck_foreign: something other than this extension has used DuckDB in the
 * current transaction (a statement pg_duckdb planned itself, a function of
 * its schema, a CALL). Its DuckDB transaction is not ours to end, and its
 * lake snapshot is not one a freshness check approved: no routing until the
 * transaction is over.
 *
 * qs_attach_open: a lake was attached inside a DuckDB transaction that has not
 * committed yet; if the PostgreSQL transaction aborts, the attachment is gone.
 *
 * qs_mirror_live: mirror nodes started and not ended. An abort with one live
 * leaves DuckDB's connection holding an open result, and every later statement
 * is refused ("connection has an open result"): qs_duck_dirty asks for the
 * instance to be recycled before the mirror is asked again.
 */
static bool qs_duck_foreign = false;
static bool qs_attach_open = false;
static int	qs_mirror_live = 0;
static bool qs_duck_dirty = false;
static HTAB *qs_mirror_hash = NULL;
static HTAB *qs_allow_hash = NULL;
static HTAB *qs_name_hash = NULL;
static HTAB *qs_watch_hash = NULL;	/* lives in TopMemoryContext: read by callbacks */
static HTAB *qs_decision_hash = NULL;
static uint64 qs_generation = 1;
static uint64 qs_load_count = 0;	/* times this backend has loaded the registry */
static uint64 qs_lsn_swept = 0;	/* qs_load_count when it last swept the applied-LSN table */
static List *qs_load_problems = NIL;	/* strings, in qs_cache_cxt */

/* per-backend state */
static int	qs_nesting = 0;
static bool qs_in_begin = false;	/* qs_record is being called by a routed plan, not the planner */
static bool qs_busy = false;
static bool qs_explain_into = false;
static bool qs_registry_moved = false;
static XLogRecPtr qs_decide_now = InvalidXLogRecPtr;	/* what the statement being planned can see */
static bool qs_decide_again = false;	/* this is the second and last attempt */

/*
 * The lake snapshot a transaction reads.
 *
 * pg_duckdb runs one DuckDB transaction per PostgreSQL transaction, and a
 * DuckLake transaction reads one snapshot of the lake: the one that was
 * current when the transaction first touched it. For a statement that is
 * planned and then run, that is when pg_duckdb PLANS it. So the applied LSNs
 * that describe what a routed statement will read are the ones reported
 * BEFORE DuckDB is first asked anything in this transaction, and a report
 * that arrives later says nothing about it: believing one answered a
 * statement from a snapshot older than the freshness check had just
 * approved (found by validate/real/freshness.py; docs/40).
 *
 * qs_pin() records every mirror's reported LSN at that moment; for the rest
 * of the transaction qs_applied_lsn() answers from the record. Waiting for a
 * mirror to catch up is therefore only possible before the pin.
 */
typedef struct QsPin
{
	Oid			heap;
	XLogRecPtr	lsn;
} QsPin;
static bool qs_xact_pinned = false;
static int	qs_npins = 0;
static int	qs_pins_cap = 0;
static QsPin *qs_pins = NULL;

/* the plan whose freshness was decided when it was made, in this transaction; used once */
static void *qs_gated_plan = NULL;
static double qs_gated_waited_ms = 0;
static LocalTransactionId qs_gated_lxid = InvalidLocalTransactionId;

typedef struct QsLast
{
	bool		valid;
	bool		fresh;			/* set since the EXPLAIN hook last looked */
	bool		mirror;
	int			reason;
	uint64		queryid;
	double		cost;
	double		rows;
	char		detail[QS_DETAIL_LEN];
	StringInfoData constructs;	/* JSON array */
} QsLast;

static QsLast qs_last;

/* hooks */
static planner_hook_type prev_planner_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static ExplainOneQuery_hook_type prev_ExplainOneQuery = NULL;
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(qs_changed);
PG_FUNCTION_INFO_V1(qs_report_applied_lsn);
PG_FUNCTION_INFO_V1(qs_mirror_status);
PG_FUNCTION_INFO_V1(qs_check);
PG_FUNCTION_INFO_V1(qs_last_decision);
PG_FUNCTION_INFO_V1(qs_forget_last);
PG_FUNCTION_INFO_V1(qs_decisions);
PG_FUNCTION_INFO_V1(qs_reasons);
PG_FUNCTION_INFO_V1(qs_reset);
PG_FUNCTION_INFO_V1(qs_allowlist_problems);
PG_FUNCTION_INFO_V1(qs_attach_lakes_sql);

/* ================================================================ constructs */

typedef struct QsConstruct
{
	const char *kind;
	char	   *ident;
	char	   *detail;
} QsConstruct;

typedef struct QsWalk
{
	List	   *constructs;		/* QsConstruct: everything not on the allowlist */
	List	   *mirrors;		/* QsMirror: relations read, each once */
	int			n_unmirrored;
	bool		has_extern_param;
	Bitmapset  *params;
} QsWalk;

static void
qs_add(QsWalk *w, const char *kind, const char *ident, const char *detail)
{
	ListCell   *lc;
	QsConstruct *c;

	foreach(lc, w->constructs)
	{
		c = (QsConstruct *) lfirst(lc);
		if (strcmp(c->kind, kind) == 0 && strcmp(c->ident, ident) == 0)
			return;
	}
	c = palloc(sizeof(QsConstruct));
	c->kind = kind;
	c->ident = pstrdup(ident);
	c->detail = detail ? pstrdup(detail) : NULL;
	w->constructs = lappend(w->constructs, c);
}

static void
qs_constructs_json(StringInfo buf, List *constructs)
{
	ListCell   *lc;
	bool		first = true;

	appendStringInfoChar(buf, '[');
	foreach(lc, constructs)
	{
		QsConstruct *c = (QsConstruct *) lfirst(lc);

		if (!first)
			appendStringInfoChar(buf, ',');
		first = false;
		appendStringInfoString(buf, "{\"kind\":");
		escape_json(buf, c->kind);
		appendStringInfoString(buf, ",\"ident\":");
		escape_json(buf, c->ident);
		if (c->detail)
		{
			appendStringInfoString(buf, ",\"detail\":");
			escape_json(buf, c->detail);
		}
		appendStringInfoChar(buf, '}');
	}
	appendStringInfoChar(buf, ']');
}

/* ================================================================ cache loading */

static void
qs_relcache_cb(Datum arg, Oid relid)
{
	qs_generation++;
	if (!qs_cache_valid)
		return;
	if (relid == InvalidOid || relid == qs_mirrors_relid || relid == qs_allowlist_relid ||
		relid == qs_lakes_relid)
		qs_cache_valid = false;
	else if (qs_watch_hash && hash_search(qs_watch_hash, &relid, HASH_FIND, NULL))
		qs_cache_valid = false;
}

static void
qs_syscache_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	qs_generation++;
	qs_cache_valid = false;
}

static void
qs_problem(const char *fmt,...) pg_attribute_printf(1, 2);

static void
qs_problem(const char *fmt,...)
{
	MemoryContext old = MemoryContextSwitchTo(qs_cache_cxt);
	char		buf[512];
	va_list		ap;

	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	qs_load_problems = lappend(qs_load_problems, pstrdup(buf));
	MemoryContextSwitchTo(old);
}

/* to_regtype / to_regoperator / to_regprocedure: InvalidOid when not found */
static Oid
qs_resolve(Oid fn, const char *ident)
{
	LOCAL_FCINFO(fcinfo, 1);
	FmgrInfo	flinfo;
	Datum		d;

	fmgr_info(fn, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 1, InvalidOid, NULL, NULL);
	fcinfo->args[0].value = CStringGetTextDatum(ident);
	fcinfo->args[0].isnull = false;
	d = FunctionCallInvoke(fcinfo);
	if (fcinfo->isnull)
		return InvalidOid;
	return DatumGetObjectId(d);
}

static char *
qs_text_attr(TupleTableSlot *slot, int attno)
{
	bool		isnull;
	Datum		d = slot_getattr(slot, attno, &isnull);

	return isnull ? NULL : TextDatumGetCString(d);
}

static bool
qs_array_has(TupleTableSlot *slot, int attno, const char *what)
{
	bool		isnull;
	Datum		d = slot_getattr(slot, attno, &isnull);
	Datum	   *elems;
	bool	   *nulls;
	int			n,
				i;

	if (isnull)
		return false;
	deconstruct_array_builtin(DatumGetArrayTypeP(d), TEXTOID, &elems, &nulls, &n);
	for (i = 0; i < n; i++)
		if (!nulls[i] && strcmp(TextDatumGetCString(elems[i]), what) == 0)
			return true;
	return false;
}

/* allowlist columns: kind, ident, collation, volatility, options, verified_by, note */
static void
qs_load_allowlist(Snapshot snap)
{
	Relation	rel = table_open(qs_allowlist_relid, AccessShareLock);
	TableScanDesc scan = table_beginscan(rel, snap, 0, NULL);
	TupleTableSlot *slot = table_slot_create(rel, NULL);

	if (RelationGetDescr(rel)->natts < 6)
	{
		qs_problem("allowlist: unexpected table shape");
		goto done;
	}
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		char	   *kind = qs_text_attr(slot, 1);
		char	   *ident = qs_text_attr(slot, 2);
		char	   *coll = qs_text_attr(slot, 3);
		char	   *vol = qs_text_attr(slot, 4);
		char	   *verified = qs_text_attr(slot, 6);
		QsAllowKey	key;
		QsAllowEntry *e;
		Oid			funcid = InvalidOid;
		bool		found;

		if (!kind || !ident || !coll || !vol)
			continue;
		if (!verified || verified[0] == '\0')
		{
			qs_problem("%s %s: no verified_by, ignored", kind, ident);
			continue;
		}
		memset(&key, 0, sizeof(key));
		if (strcmp(kind, "node") == 0 || strcmp(kind, "feature") == 0)
		{
			char		name[64];

			/* a node this code does not know is never safe, whatever the table says */
			if (strncmp(ident, "tag", 3) == 0 || strlen(kind) + strlen(ident) + 2 > sizeof(name))
			{
				qs_problem("%s %s: cannot be allowlisted", kind, ident);
				continue;
			}
			memset(name, 0, sizeof(name));
			snprintf(name, sizeof(name), "%s:%s", kind, ident);
			hash_search(qs_name_hash, name, HASH_ENTER, NULL);
			continue;
		}
		else if (strcmp(kind, "type") == 0)
		{
			key.kind = QA_TYPE;
			key.oid = qs_resolve(F_TO_REGTYPE, ident);
		}
		else if (strcmp(kind, "operator") == 0)
		{
			key.kind = QA_OPER;
			key.oid = qs_resolve(F_TO_REGOPERATOR, ident);
			if (OidIsValid(key.oid))
				funcid = get_opcode(key.oid);
		}
		else if (strcmp(kind, "function") == 0 || strcmp(kind, "aggregate") == 0 ||
				 strcmp(kind, "window") == 0)
		{
			char		prokind;

			key.kind = kind[0] == 'f' ? QA_FUNC : kind[0] == 'a' ? QA_AGG : QA_WINDOW;
			key.oid = funcid = qs_resolve(F_TO_REGPROCEDURE, ident);
			if (OidIsValid(funcid))
			{
				prokind = get_func_prokind(funcid);
				if ((key.kind == QA_FUNC && prokind != PROKIND_FUNCTION) ||
					(key.kind == QA_AGG && prokind != PROKIND_AGGREGATE) ||
					(key.kind == QA_WINDOW && prokind != PROKIND_WINDOW && prokind != PROKIND_AGGREGATE))
				{
					qs_problem("%s %s: is not a %s", kind, ident, kind);
					continue;
				}
				if (key.kind != QA_FUNC)
					funcid = InvalidOid;	/* aggregates: volatility is the transition's, immutable by rule */
			}
		}
		else if (strcmp(kind, "cast") == 0)
		{
			char	   *arrow = strstr(ident, "->");

			key.kind = QA_CAST;
			if (arrow)
			{
				char	   *src = pnstrdup(ident, arrow - ident);

				key.oid = qs_resolve(F_TO_REGTYPE, src);
				key.oid2 = qs_resolve(F_TO_REGTYPE, arrow + 2);
				if (!OidIsValid(key.oid2))
					key.oid = InvalidOid;
			}
		}
		else
		{
			qs_problem("%s %s: unknown kind", kind, ident);
			continue;
		}
		if (!OidIsValid(key.oid))
		{
			qs_problem("%s %s: does not resolve in pg_catalog", kind, ident);
			continue;
		}

		/*
		 * A function that is not immutable depends on the session (time zone,
		 * DateStyle, the clock, search_path). It is admitted only by an entry
		 * that says so: somebody has to have looked at that dependence.
		 */
		if (OidIsValid(funcid))
		{
			char		actual = func_volatile(funcid);

			if ((actual == PROVOLATILE_STABLE && strcmp(vol, "immutable") == 0) ||
				(actual == PROVOLATILE_VOLATILE && strcmp(vol, "volatile") != 0))
			{
				qs_problem("%s %s: is %s but the entry says %s", kind, ident,
						   actual == PROVOLATILE_STABLE ? "stable" : "volatile", vol);
				continue;
			}
		}

		e = hash_search(qs_allow_hash, &key, HASH_ENTER, &found);
		e->coll = strcmp(coll, "any") == 0 ? QC_ANY :
			strcmp(coll, "deterministic") == 0 ? QC_DETERMINISTIC :
			strcmp(coll, "c") == 0 ? QC_C : QC_NONE;
		e->need_typmod = qs_array_has(slot, 5, "typmod");
		e->finite = qs_array_has(slot, 5, "finite");
		e->io = qs_array_has(slot, 5, "io");
		e->cmp = qs_array_has(slot, 5, "cmp");
		e->cmp_only = qs_array_has(slot, 5, "cmp_only");
		e->errors = qs_array_has(slot, 5, "errors");
	}
done:
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	table_close(rel, AccessShareLock);
}

static void
qs_watch(Oid relid)
{
	if (OidIsValid(relid))
		hash_search(qs_watch_hash, &relid, HASH_ENTER, NULL);
}

static QsAllowEntry *qs_allow_lookup(QsAllowKind kind, Oid oid, Oid oid2);
static bool qs_must_rethrow(int sqlerrcode);
static bool qs_mirror_error_ok(ErrorData *ed);

/* How a mirror column of type (mtype, mmod) becomes the heap's (htype, hmod). */
static bool
qs_storage_cast_ok(Oid htype, int32 hmod, Oid mtype, int32 mmod, bool *need_cast)
{
	*need_cast = true;
	if (htype == mtype)
	{
		if (hmod == mmod)
		{
			*need_cast = false;
			return true;
		}
		return hmod >= 0;		/* the mirror is wider; narrowing back is exact */
	}
	if ((htype == INT2OID || htype == INT4OID) && mtype == INT8OID)
		return true;
	if (htype == INT2OID && mtype == INT4OID)
		return true;
	if (htype == UUIDOID && mtype == TEXTOID)
		return true;
	return false;
}

static void
qs_build_mirror(QsMirror *m)
{
	Relation	heap;
	TupleDesc	desc;
	StringInfoData sql;
	int			i;
	HeapTuple	tup;
	Form_pg_class cls;
	char		mkind;
	Oid			mowner;
	List	   *raw,
			   *rewritten;
	Query	   *q;
	MemoryContext old;
	ListCell   *lc;

	m->usable = false;
	heap = try_table_open(m->heap, AccessShareLock);
	if (heap == NULL)
	{
		m->reason = "heap relation does not exist";
		return;
	}
	m->heapname = MemoryContextStrdup(qs_cache_cxt,
									  quote_qualified_identifier(get_namespace_name(RelationGetNamespace(heap)),
																 RelationGetRelationName(heap)));
	desc = RelationGetDescr(heap);
	m->natts = desc->natts;
	m->col_ok = MemoryContextAllocZero(qs_cache_cxt, sizeof(bool) * Max(desc->natts, 1));
	m->col_why = MemoryContextAllocZero(qs_cache_cxt, sizeof(char *) * Max(desc->natts, 1));
	m->rls = heap->rd_rel->relrowsecurity;

	if (heap->rd_rel->relkind != RELKIND_RELATION)
		m->reason = "heap relation is not a plain table (partitioned, view or foreign)";
	else if (heap->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		m->reason = "heap relation is temporary or unlogged";
	else if (IsCatalogRelation(heap))
		m->reason = "heap relation is a system catalog";
	else if (GetDatabaseEncoding() != PG_UTF8)
		m->reason = "database encoding is not UTF8";
	if (m->reason)
		goto out;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(m->mirror));
	if (!HeapTupleIsValid(tup))
	{
		m->reason = "mirror relation does not exist";
		goto out;
	}
	cls = (Form_pg_class) GETSTRUCT(tup);
	mkind = cls->relkind;
	mowner = cls->relowner;
	initStringInfo(&sql);
	appendStringInfoString(&sql, "SELECT ");
	ReleaseSysCache(tup);
	if (mkind != RELKIND_VIEW && mkind != RELKIND_RELATION && mkind != RELKIND_FOREIGN_TABLE &&
		mkind != RELKIND_MATVIEW)
	{
		m->reason = "mirror relation is not a view or table";
		goto out;
	}
	if (m->mirror == m->heap)
	{
		m->reason = "mirror relation is the heap itself";
		goto out;
	}

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		AttrNumber	mattno;
		Oid			mtype;
		int32		mmod;
		Oid			mcoll;
		bool		need_cast = false;
		QsAllowEntry *ae;

		if (i > 0)
			appendStringInfoString(&sql, ", ");
		if (att->attisdropped)
		{
			appendStringInfo(&sql, "NULL::int4 AS \"qs_dropped_%d\"", i + 1);
			m->col_why[i] = "dropped";
			continue;
		}
		/* by name: the heap's attribute numbers mean nothing to the mirror */
		mattno = get_attnum(m->mirror, NameStr(att->attname));
		if (mattno <= 0)
		{
			m->col_why[i] = "not in the mirror";
			appendStringInfo(&sql, "NULL::int4 AS %s", quote_identifier(NameStr(att->attname)));
			continue;
		}
		get_atttypetypmodcoll(m->mirror, mattno, &mtype, &mmod, &mcoll);
		if (att->attgenerated)
			m->col_why[i] = "generated column";
		else if ((ae = qs_allow_lookup(QA_TYPE, att->atttypid, InvalidOid)) != NULL && ae->finite)
		{
			ListCell   *lf;
			bool		asserted = false;

			foreach(lf, m->finite_cols)
				if (strcmp((char *) lfirst(lf), NameStr(att->attname)) == 0)
					asserted = true;
			if (!asserted)
				m->col_why[i] = "may hold infinity, which the mirror does not keep (not in finite_columns)";
		}
		if (m->col_why[i])
			;
		else if (!qs_storage_cast_ok(att->atttypid, att->atttypmod, mtype, mmod, &need_cast))
			m->col_why[i] = "stored as a different type in the mirror";
		if (m->col_why[i])
		{
			appendStringInfo(&sql, "NULL::int4 AS %s", quote_identifier(NameStr(att->attname)));
			continue;
		}
		m->col_ok[i] = true;
		if (need_cast)
			appendStringInfo(&sql, "CAST(m.%s AS %s) AS %s", quote_identifier(NameStr(att->attname)),
							 format_type_with_typemod(att->atttypid, att->atttypmod),
							 quote_identifier(NameStr(att->attname)));
		else
			appendStringInfo(&sql, "m.%s", quote_identifier(NameStr(att->attname)));
	}
	if (desc->natts == 0)
		appendStringInfoString(&sql, "NULL::int4");
	appendStringInfo(&sql, " FROM %s m",
					 quote_qualified_identifier(get_namespace_name(get_rel_namespace(m->mirror)),
												get_rel_name(m->mirror)));

	/*
	 * The columns and types were checked against the catalog above, so this
	 * analysis is not expected to fail; if it does anyway, the mirror is
	 * unusable and the statement that triggered the load is not disturbed.
	 */
	old = CurrentMemoryContext;
	PG_TRY();
	{
		raw = pg_parse_query(sql.data);
		rewritten = pg_analyze_and_rewrite_fixedparams(linitial_node(RawStmt, raw), sql.data, NULL, 0, NULL);
		if (list_length(rewritten) != 1)
			elog(ERROR, "mirror view rewrites to %d statements", list_length(rewritten));
		q = linitial_node(Query, rewritten);
		if (q->commandType != CMD_SELECT || list_length(q->targetList) != Max(desc->natts, 1))
			elog(ERROR, "mirror view does not rewrite to a plain SELECT");
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		MemoryContextSwitchTo(old);
		ed = CopyErrorData();
		FlushErrorState();
		m->reason = MemoryContextStrdup(qs_cache_cxt, psprintf("mirror relation cannot be read: %s", ed->message));
		q = NULL;
	}
	PG_END_TRY();
	if (q == NULL)
		goto out;

	i = 0;
	foreach(lc, q->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (i < desc->natts && m->col_ok[i])
		{
			Form_pg_attribute att = TupleDescAttr(desc, i);

			if (exprType((Node *) tle->expr) != att->atttypid || exprTypmod((Node *) tle->expr) != att->atttypmod)
			{
				m->col_ok[i] = false;
				m->col_why[i] = "stored as a different type in the mirror";
			}
		}
		i++;
	}

	/*
	 * The substituted subquery reads the mirror with the mirror owner's
	 * rights, as a view would: users are not expected to hold any privilege
	 * on the mirror relation itself. What they may read is decided by the
	 * heap table's own permission entries, which stay in the routed plan.
	 */
	foreach(lc, q->rteperminfos)
	{
		RTEPermissionInfo *pi = lfirst_node(RTEPermissionInfo, lc);

		if (!OidIsValid(pi->checkAsUser))
			pi->checkAsUser = mowner;
	}
	old = MemoryContextSwitchTo(qs_cache_cxt);
	m->subquery = copyObject(q);
	MemoryContextSwitchTo(old);
	m->usable = true;
out:
	table_close(heap, AccessShareLock);
}

/* mirrors columns: heap, mirror, applied_lsn, max_lag_bytes, enabled, finite_columns */
static void
qs_load_mirrors(Snapshot snap)
{
	Relation	rel = table_open(qs_mirrors_relid, AccessShareLock);
	TableScanDesc scan = table_beginscan(rel, snap, 0, NULL);
	TupleTableSlot *slot = table_slot_create(rel, NULL);
	List	   *todo = NIL;
	ListCell   *lc;

	if (RelationGetDescr(rel)->natts >= 6)
	{
		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			bool		n1,
						n2,
						n3,
						n4,
						n5;
			Oid			heap = DatumGetObjectId(slot_getattr(slot, 1, &n1));
			Oid			mirror = DatumGetObjectId(slot_getattr(slot, 2, &n2));
			XLogRecPtr	lsn = DatumGetLSN(slot_getattr(slot, 3, &n3));
			int64		lag = DatumGetInt64(slot_getattr(slot, 4, &n4));
			bool		enabled = DatumGetBool(slot_getattr(slot, 5, &n5));
			QsMirror   *m;
			bool		found;

			if (n1 || n2 || !OidIsValid(heap))
				continue;
			m = hash_search(qs_mirror_hash, &heap, HASH_ENTER, &found);
			memset(((char *) m) + sizeof(Oid), 0, sizeof(QsMirror) - sizeof(Oid));
			m->mirror = mirror;
			m->table_lsn = n3 ? InvalidXLogRecPtr : lsn;
			m->max_lag = n4 ? -1 : lag;
			{
				bool		n6;
				Datum		d6 = slot_getattr(slot, 6, &n6);

				if (!n6)
				{
					Datum	   *elems;
					bool	   *nulls;
					int			n,
								k;
					MemoryContext old = MemoryContextSwitchTo(qs_cache_cxt);

					deconstruct_array(DatumGetArrayTypeP(d6), NAMEOID, NAMEDATALEN, false, TYPALIGN_CHAR, &elems, &nulls, &n);
					for (k = 0; k < n; k++)
						if (!nulls[k])
							m->finite_cols = lappend(m->finite_cols, pstrdup(NameStr(*DatumGetName(elems[k]))));
					MemoryContextSwitchTo(old);
				}
			}
			qs_watch(heap);
			qs_watch(mirror);
			if (n5 || !enabled)
				m->reason = "disabled in quicksilver.mirrors";
			else
				todo = lappend(todo, m);
		}
	}
	else
		qs_problem("mirrors: unexpected table shape");
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	/* hash entries do not move: nothing is added after this point */
	foreach(lc, todo)
		qs_build_mirror((QsMirror *) lfirst(lc));
}

/* lakes columns: alias, catalog */
static void
qs_load_lakes(Snapshot snap)
{
	Relation	rel;
	TableScanDesc scan;
	TupleTableSlot *slot;

	if (!OidIsValid(qs_lakes_relid))
		return;
	rel = table_open(qs_lakes_relid, AccessShareLock);
	if (RelationGetDescr(rel)->natts < 2)
	{
		table_close(rel, AccessShareLock);
		qs_problem("lakes: unexpected table shape");
		return;
	}
	scan = table_beginscan(rel, snap, 0, NULL);
	slot = table_slot_create(rel, NULL);
	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		bool		n1,
					n2;
		Datum		d1 = slot_getattr(slot, 1, &n1);
		Datum		d2 = slot_getattr(slot, 2, &n2);
		MemoryContext old;
		QsLake	   *lake;

		if (n1 || n2)
			continue;
		old = MemoryContextSwitchTo(qs_cache_cxt);
		lake = palloc(sizeof(QsLake));
		lake->alias = pstrdup(NameStr(*DatumGetName(d1)));
		lake->catalog = TextDatumGetCString(d2);
		qs_lakes = lappend(qs_lakes, lake);
		MemoryContextSwitchTo(old);
	}
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	table_close(rel, AccessShareLock);
}

/* ---------------------------------------------------------------- pg_duckdb, on behalf of somebody else */

static bool qs_mirror_error_ok(ErrorData *ed);

/*
 * The mirror side runs as the bootstrap superuser, and only the mirror side.
 *
 * Stock pg_duckdb gives DuckDB to superusers and to members of
 * duckdb.postgres_role, and it gives the local file system to superusers
 * only. A member of that role can also run any DuckDB statement it likes
 * (duckdb.query, duckdb.raw_query), and DuckDB knows nothing of PostgreSQL's
 * privileges: with the lake attached it would read every mirrored table,
 * whatever it may read from the heap. So the application's role is given
 * nothing: no membership, no privilege on a mirror relation. The statement
 * DuckDB runs for it is one this extension wrote, after the executor's own
 * check of the heap table's privileges, out of constructs on the allowlist;
 * nothing of the user's is evaluated by PostgreSQL in between (the mirror
 * statement holds allowlisted pg_catalog objects only, and DuckDB evaluates
 * them). The identity is switched around each call into pg_duckdb and
 * switched back before a row is returned.
 */
typedef struct QsElev
{
	bool		on;
	Oid			uid;
	int			sec;
} QsElev;

static inline void
qs_elev_begin(QsElev *e)
{
	e->on = !superuser();
	if (e->on)
	{
		GetUserIdAndSecContext(&e->uid, &e->sec);
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID, e->sec | SECURITY_LOCAL_USERID_CHANGE);
	}
}

static inline void
qs_elev_end(QsElev *e)
{
	if (e->on)
		SetUserIdAndSecContext(e->uid, e->sec);
}

/*
 * May a session that is not a superuser's be answered from the mirror?
 *
 * No. With stock pg_duckdb 1.2.0 a role that is not a superuser gets no local
 * file system from its DuckDB (measured: "File system LocalFileSystem has been
 * disabled by configuration", with duckdb.enable_external_access = off and
 * duckdb.allowed_directories set). Its DuckDB therefore cannot read the lake,
 * and pg_duckdb refuses a role that is not a member of duckdb.postgres_role
 * outright. Membership would open DuckDB to the role, which can then run any
 * statement it likes and would see every mirrored table, whatever it may read
 * from the heap. So the statement stays on the heap, which answers it exactly.
 * docs/40 says what an operator would need, and why that is not enough.
 */
static bool
qs_mirror_allowed(char *why, size_t whylen)
{
	if (superuser())
		return true;
	strlcpy(why, "only superusers are routed: stock pg_duckdb gives DuckDB's local file system to superusers only", whylen);
	return false;
}

/*
 * Run one statement in this backend's DuckDB (duckdb.raw_query), as the
 * bootstrap superuser. An error DuckDB raised is swallowed and reported as
 * false; anything else is rethrown.
 */
static bool
qs_duck_raw(const char *sql)
{
	Oid			argtypes[1] = {TEXTOID};
	Oid			fn;
	MemoryContext old = CurrentMemoryContext;
	int			save_nestlevel;
	bool		save_busy = qs_busy;
	bool		ok = true;
	QsElev		elev;

	qs_elev_begin(&elev);
	fn = LookupFuncName(list_make2(makeString("duckdb"), makeString("raw_query")), 1, argtypes, true);
	if (!OidIsValid(fn))
	{
		qs_elev_end(&elev);
		return false;			/* pg_duckdb is not in this database */
	}
	save_nestlevel = NewGUCNestLevel();
	qs_busy = true;
	PG_TRY();
	{
		/* raw_query reports its result as a NOTICE */
		(void) set_config_option("client_min_messages", "warning", PGC_SUSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
		(void) set_config_option("log_min_messages", "warning", PGC_SUSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
		OidFunctionCall1(fn, CStringGetTextDatum(sql));
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		MemoryContextSwitchTo(old);
		ed = CopyErrorData();
		if (!qs_mirror_error_ok(ed))
		{
			AtEOXact_GUC(true, save_nestlevel);
			qs_busy = save_busy;
			qs_elev_end(&elev);
			PG_RE_THROW();
		}
		FlushErrorState();
		FreeErrorData(ed);
		ok = false;
	}
	PG_END_TRY();
	qs_busy = save_busy;
	AtEOXact_GUC(true, save_nestlevel);
	qs_elev_end(&elev);
	return ok;
}

/*
 * Would a DuckDB transaction opened now outlive the statement? In a
 * transaction block, an implicit one (several statements in one message, a
 * pipeline) included: PostgreSQL only ends it later, and pg_duckdb only ends
 * DuckDB's when PostgreSQL does.
 */
static bool
qs_in_block(void)
{
	return IsInTransactionBlock(true);
}

/*
 * End the DuckDB transaction pg_duckdb opened for a statement of ours.
 *
 * pg_duckdb keeps one DuckDB transaction open from the first DuckDB statement
 * in a transaction block to the block's end, and while it is open it raises
 * an error from its subtransaction callback when a SAVEPOINT is taken (the
 * transaction is then aborted) or a PL/pgSQL exception block is entered (the
 * session is then lost: the error is raised where PostgreSQL cannot take
 * one). A routed read needs none of that: it has read, and holds nothing. So
 * its DuckDB transaction is ended with the statement, and what follows in the
 * block (a savepoint, a write, another routed read, which then reads a new
 * snapshot of the lake as a READ COMMITTED statement reads a new snapshot of
 * the heap) finds DuckDB idle.
 *
 * Returns false if DuckDB's transaction turns out to have been invalidated:
 * see below. `failed` says the caller already knows (DuckDB raised an error),
 * and the transaction is only rolled back.
 */
static bool
qs_duck_end(bool failed)
{
	bool		ok = !failed;

	if (qs_duck_foreign)
		return true;			/* not ours to end */

	/*
	 * Did the statement DuckDB ran last finish? pg_duckdb streams a result,
	 * and when DuckDB fails after the first rows (a value it cannot cast in
	 * row 150,000, a data file that has gone) the stream just ends: no error
	 * is raised, the rows so far look like the whole answer. (Stock pg_duckdb
	 * 1.2.0, with no router involved; errors.sql has the statement.) What
	 * DuckDB does do is invalidate its transaction, and the next statement in
	 * it is refused. So one is sent: if it is refused, the result that came
	 * before it was cut short.
	 */
	if (ok)
		ok = qs_duck_raw("SELECT 1");
	if (!ok || !qs_duck_raw("COMMIT"))
		(void) qs_duck_raw("ROLLBACK");
	if (ok)
		qs_attach_open = false;
	else if (qs_attach_open)
	{
		qs_lakes_attached = false;
		qs_attach_open = false;
	}
	/* what the next statement reads is decided by its own freshness check */
	qs_xact_pinned = false;
	qs_npins = 0;
	return ok;
}

/* A DuckDB connection left holding an open result by an abort is replaced. */
static void
qs_duck_recycle(void)
{
	Oid			fn;
	QsElev		elev;
	bool		save_busy = qs_busy;

	if (!qs_duck_dirty)
		return;
	qs_duck_dirty = false;
	qs_elev_begin(&elev);
	fn = LookupFuncName(list_make2(makeString("duckdb"), makeString("recycle_ddb")), 0, NULL, true);
	if (OidIsValid(fn))
	{
		qs_busy = true;
		PG_TRY();
		{
			OidFunctionCall0(fn);
		}
		PG_FINALLY();
		{
			qs_busy = save_busy;
			qs_elev_end(&elev);
		}
		PG_END_TRY();
	}
	else
		qs_elev_end(&elev);
	qs_lakes_attached = false;
	qs_attach_retry_at = 0;
}

/*
 * Attach every registered lake whose catalog file is on this node, in this
 * backend's DuckDB. Idempotent (ATTACH IF NOT EXISTS, READ_ONLY) and never an
 * error: a lake that cannot be attached leaves its mirror relations
 * unplannable, which is "heap". Returns how many lakes are attached, or -1 if
 * one of them is not. The caller has established that this session may be
 * given the mirror (qs_mirror_allowed).
 */
static int
qs_attach_lakes(bool force)
{
	ListCell   *lc;
	int			n = 0;
	bool		failed = false;
	bool		refused = false;

	if (qs_lakes == NIL)
		return 0;
	if (qs_lakes_attached && !force)
		return list_length(qs_lakes);
	if (!force && qs_attach_retry_at != 0 && GetCurrentTimestamp() < qs_attach_retry_at)
		return -1;
	foreach(lc, qs_lakes)
	{
		QsLake	   *lake = (QsLake *) lfirst(lc);
		struct stat sb;
		StringInfoData sql;

		if (stat(lake->catalog, &sb) != 0)
		{
			failed = true;		/* no mirror on this node (the primary), or not yet */
			continue;
		}
		initStringInfo(&sql);
		appendStringInfo(&sql, "ATTACH IF NOT EXISTS %s AS %s (READ_ONLY)",
						 quote_literal_cstr(psprintf("ducklake:sqlite:%s", lake->catalog)),
						 quote_identifier(lake->alias));
		if (qs_duck_raw(sql.data))
			n++;
		else
			failed = refused = true;
	}
	qs_lakes_attached = !failed;
	if (n > 0)
		qs_attach_open = true;
	/* a lake that is not there is asked about again, but not by every statement */
	/*
	 * ... and one DuckDB refused is not going to be accepted a few seconds
	 * later, and the refusal itself can take seconds.
	 */
	qs_attach_retry_at = failed ? TimestampTzPlusMilliseconds(GetCurrentTimestamp(), refused ? 300000 : 5000) : 0;
	return failed ? -1 : n;
}

static Oid
qs_own_table(Oid nsp, Oid ext, const char *name)
{
	Oid			relid = get_relname_relid(name, nsp);

	/* only the tables CREATE EXTENSION made: a look-alike schema is not obeyed */
	if (!OidIsValid(relid) || getExtensionOfObject(RelationRelationId, relid) != ext)
		return InvalidOid;
	return relid;
}

/*
 * Make the registry and the allowlist current. Called only at entry points,
 * where nothing holds a pointer into the cache: an invalidation only clears
 * the flag, the memory is replaced here.
 */
static void
qs_ensure_loaded(void)
{
	HASHCTL		ctl;
	Oid			nsp,
				ext;
	Snapshot	snap;
	int			save_nestlevel;
	bool		save_busy = qs_busy;
	Oid			save_userid;
	int			save_sec_context;

	if (qs_cache_valid)
		return;

	if (qs_watch_hash == NULL)
	{
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(QsWatch);
		ctl.hcxt = TopMemoryContext;
		qs_watch_hash = hash_create("quicksilver watch", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(uint64);
		ctl.entrysize = sizeof(QsDecisionCache);
		ctl.hcxt = TopMemoryContext;
		qs_decision_hash = hash_create("quicksilver decisions", 256, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	else
	{
		HASH_SEQ_STATUS seq;
		QsWatch    *w;

		hash_seq_init(&seq, qs_watch_hash);
		while ((w = hash_seq_search(&seq)) != NULL)
			hash_search(qs_watch_hash, &w->relid, HASH_REMOVE, NULL);
	}

	qs_mirror_hash = qs_allow_hash = qs_name_hash = NULL;
	qs_load_problems = NIL;
	qs_installed = false;
	qs_mirrors_relid = qs_allowlist_relid = qs_lakes_relid = InvalidOid;
	qs_lakes = NIL;
	qs_lakes_attached = false;
	if (qs_cache_cxt)
		MemoryContextReset(qs_cache_cxt);
	else
		qs_cache_cxt = AllocSetContextCreate(TopMemoryContext, "quicksilver cache", ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(QsMirror);
	ctl.hcxt = qs_cache_cxt;
	qs_mirror_hash = hash_create("quicksilver mirrors", 32, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QsAllowKey);
	ctl.entrysize = sizeof(QsAllowEntry);
	ctl.hcxt = qs_cache_cxt;
	qs_allow_hash = hash_create("quicksilver allowlist", 256, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(((QsNameEntry *) 0)->name);
	ctl.entrysize = sizeof(QsNameEntry);
	ctl.hcxt = qs_cache_cxt;
	qs_name_hash = hash_create("quicksilver features", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* set first: an invalidation arriving during the load clears it again */
	qs_cache_valid = true;
	qs_generation++;
	qs_load_count++;

	nsp = get_namespace_oid(QS_SCHEMA, true);
	ext = get_extension_oid(QS_EXTENSION, true);
	if (!OidIsValid(nsp) || !OidIsValid(ext))
		return;
	qs_mirrors_relid = qs_own_table(nsp, ext, "mirrors");
	qs_allowlist_relid = qs_own_table(nsp, ext, "allowlist");
	if (!OidIsValid(qs_mirrors_relid) || !OidIsValid(qs_allowlist_relid))
		return;
	qs_installed = true;
	qs_lakes_relid = qs_own_table(nsp, ext, "lakes");

	/*
	 * Names in the allowlist are resolved with pg_catalog alone on the path,
	 * so the session's search_path cannot make "=(text,text)" mean something
	 * else. The resolved identity is the OID; the spelling is forgotten.
	 */
	/*
	 * And as the bootstrap superuser: the registry is the administrator's
	 * statement of what mirrors what, and reading it must not depend on what
	 * the session's user may look up (they hold no privilege on the mirror
	 * relations or their schemas, by design). Nothing of the user's runs
	 * here: the analysed text is a column list this code wrote.
	 */
	qs_busy = true;
	save_nestlevel = NewGUCNestLevel();
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	PG_TRY();
	{
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   save_sec_context | SECURITY_LOCAL_USERID_CHANGE | SECURITY_RESTRICTED_OPERATION);
		(void) set_config_option("search_path", "pg_catalog, pg_temp", PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
		snap = RegisterSnapshot(GetLatestSnapshot());
		qs_load_allowlist(snap);
		qs_load_mirrors(snap);
		qs_load_lakes(snap);
		UnregisterSnapshot(snap);
	}
	PG_FINALLY();
	{
		AtEOXact_GUC(true, save_nestlevel);
		SetUserIdAndSecContext(save_userid, save_sec_context);
		qs_busy = save_busy;
	}
	PG_END_TRY();
}

static QsMirror *
qs_mirror_lookup(Oid relid)
{
	if (!qs_mirror_hash)
		return NULL;
	return hash_search(qs_mirror_hash, &relid, HASH_FIND, NULL);
}

static QsAllowEntry *
qs_allow_lookup(QsAllowKind kind, Oid oid, Oid oid2)
{
	QsAllowKey	key;

	memset(&key, 0, sizeof(key));
	key.kind = kind;
	key.oid = oid;
	key.oid2 = oid2;
	return hash_search(qs_allow_hash, &key, HASH_FIND, NULL);
}

static bool
qs_name_allowed(const char *kind, const char *ident)
{
	char		name[64];

	memset(name, 0, sizeof(name));
	snprintf(name, sizeof(name), "%s:%s", kind, ident);
	return hash_search(qs_name_hash, name, HASH_FIND, NULL) != NULL;
}

/* ================================================================ the safety walk */

static bool qs_expr_walker(Node *node, void *context);
static void qs_walk_query(Query *q, QsWalk *w);
static bool qs_must_rethrow(int sqlerrcode);
static bool qs_mirror_error_ok(ErrorData *ed);

static void
qs_need(QsWalk *w, const char *kind, const char *ident)
{
	if (!qs_name_allowed(kind, ident))
		qs_add(w, kind, ident, NULL);
}

static char *
qs_type_name(Oid typid)
{
	return format_type_extended(typid, -1, FORMAT_TYPE_FORCE_QUALIFY | FORMAT_TYPE_ALLOW_INVALID);
}

static void
qs_check_type(QsWalk *w, Oid typid, const char *where)
{
	if (!qs_allow_lookup(QA_TYPE, typid, InvalidOid))
		qs_add(w, "type", qs_type_name(typid), where);
}

static void
qs_check_coll(QsWalk *w, QsAllowEntry *e, Oid collid, const char *what)
{
	bool		ok;
	const char *need;

	switch (e->coll)
	{
		case QC_ANY:
			return;
		case QC_DETERMINISTIC:
			ok = !OidIsValid(collid) || get_collation_isdeterministic(collid);
			need = "needs a deterministic collation";
			break;
		case QC_C:
			ok = OidIsValid(collid) && lc_collate_is_c(collid);
			need = "byte order is the mirror's only order: needs the C collation";
			break;
		default:
			ok = !OidIsValid(collid);
			need = "entry does not cover collatable input";
			break;
	}
	if (!ok)
	{
		char	   *cn = OidIsValid(collid) ? get_collation_name(collid) : NULL;

		qs_add(w, "collation", psprintf("%s under %s", what, cn ? cn : "no collation"), need);
	}
}

static void
qs_check_oper(QsWalk *w, Oid opno, Oid collid)
{
	QsAllowEntry *e = qs_allow_lookup(QA_OPER, opno, InvalidOid);
	char	   *name = format_operator_extended(opno, FORMAT_OPERATOR_FORCE_QUALIFY | FORMAT_OPERATOR_INVALID_AS_NULL);

	if (name == NULL)
		name = psprintf("operator %u", opno);
	if (!e)
		qs_add(w, "operator", name, NULL);
	else
	{
		qs_check_coll(w, e, collid, name);

		/*
		 * i + 1 raises "integer out of range" in PostgreSQL. DuckDB raises it
		 * too when it evaluates i + 1, but its optimizer need not: it turns
		 * (i + 1 > 0) into (i > -1) and sum(i + 1) into sum(i) + count(i).
		 * Where the heap would have failed, the mirror then answers.
		 */
		if (e->errors && !qs_tolerate_lost_errors)
			qs_add(w, "operator", name,
				   "PostgreSQL can raise an error here that the mirror's optimizer may skip; quicksilver.tolerate_lost_errors");
	}
}

static void
qs_check_func(QsWalk *w, QsAllowKind kind, Oid funcid, Oid collid)
{
	QsAllowEntry *e = qs_allow_lookup(kind, funcid, InvalidOid);
	char	   *name = format_procedure_extended(funcid, FORMAT_PROC_FORCE_QUALIFY | FORMAT_PROC_INVALID_AS_NULL);

	if (name == NULL)
		name = psprintf("function %u", funcid);
	if (!e)
		qs_add(w, kind == QA_FUNC ? "function" : kind == QA_AGG ? "aggregate" : "window", name, NULL);
	else
		qs_check_coll(w, e, collid, name);
}

/* ORDER BY / GROUP BY / DISTINCT / PARTITION BY items */
static void
qs_check_sortgroup(QsWalk *w, List *clauses, List *tlist, bool ordering, bool equality)
{
	ListCell   *lc;

	foreach(lc, clauses)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle;
		Oid			coll;

		if (!IsA(sgc, SortGroupClause))
		{
			qs_add(w, "feature", "grouping_sets", NULL);
			continue;
		}
		tle = get_sortgroupclause_tle(sgc, tlist);
		coll = exprCollation((Node *) tle->expr);
		if (ordering)
		{
			if (OidIsValid(sgc->sortop))
				qs_check_oper(w, sgc->sortop, coll);
			else
				qs_add(w, "feature", "unsortable_order", NULL);
		}
		if (equality)
		{
			if (OidIsValid(sgc->eqop))
				qs_check_oper(w, sgc->eqop, coll);
			else
				qs_add(w, "feature", "unhashable_group", NULL);
		}
	}
}

/*
 * PostgreSQL's numeric carries its scale in each value; the mirror's decimal
 * carries it in the column. A literal PostgreSQL prints without a decimal
 * point, or has to quote, reaches DuckDB as DECIMAL(18,3) and drags every
 * result it touches to three decimals.
 */
static void
qs_check_const(QsWalk *w, Const *c)
{
	QsAllowEntry *e = qs_allow_lookup(QA_TYPE, c->consttype, InvalidOid);
	char	   *txt;
	bool		ok;

	if (e == NULL || !e->need_typmod)
		return;
	if (c->constisnull)
	{
		qs_add(w, "constant", "NULL of a scaled type", "the mirror would give it a scale of its own");
		return;
	}
	if (c->consttype != NUMERICOID)
		return;
	txt = DatumGetCString(DirectFunctionCall1(numeric_out, c->constvalue));
	ok = isdigit((unsigned char) txt[0]) && strchr(txt, '.') != NULL &&
		strspn(txt, "0123456789.") == strlen(txt);
	if (!ok)
		qs_add(w, "constant", "numeric literal without a decimal point",
			   "the mirror would give it three decimals; write 2.0, or compare with it");
}

/* CASE, COALESCE, GREATEST/LEAST: values of several scales in one column */
static void
qs_check_merge(QsWalk *w, Oid typid, int32 typmod, const char *what)
{
	QsAllowEntry *e = qs_allow_lookup(QA_TYPE, typid, InvalidOid);

	if (e && e->need_typmod && typmod < 0)
		qs_add(w, "feature", psprintf("%s merging %s of different scales", what, qs_type_name(typid)),
			   "PostgreSQL keeps each value's scale, the mirror one per column");
}

static const char *
qs_node_name(NodeTag tag)
{
	switch (tag)
	{
		case T_BoolExpr:
			return "BoolExpr";
		case T_NullTest:
			return "NullTest";
		case T_BooleanTest:
			return "BooleanTest";
		case T_CaseExpr:
			return "CaseExpr";
		case T_CoalesceExpr:
			return "CoalesceExpr";
		case T_MinMaxExpr:
			return "MinMaxExpr";
		case T_NullIfExpr:
			return "NullIfExpr";
		case T_DistinctExpr:
			return "DistinctExpr";
		case T_CollateExpr:
			return "CollateExpr";
		case T_ArrayExpr:
			return "ArrayExpr";
		case T_RowExpr:
			return "RowExpr";
		case T_RowCompareExpr:
			return "RowCompareExpr";
		case T_SQLValueFunction:
			return "SQLValueFunction";
		case T_CoerceToDomain:
			return "CoerceToDomain";
		case T_ArrayCoerceExpr:
			return "ArrayCoerceExpr";
		case T_ConvertRowtypeExpr:
			return "ConvertRowtypeExpr";
		case T_FieldSelect:
			return "FieldSelect";
		case T_SubscriptingRef:
			return "SubscriptingRef";
		case T_GroupingFunc:
			return "GroupingFunc";
		case T_NamedArgExpr:
			return "NamedArgExpr";
		case T_XmlExpr:
			return "XmlExpr";
		case T_JsonValueExpr:
			return "JsonValueExpr";
		case T_JsonConstructorExpr:
			return "JsonConstructorExpr";
		case T_JsonIsPredicate:
			return "JsonIsPredicate";
		case T_JsonExpr:
			return "JsonExpr";
		case T_NextValueExpr:
			return "NextValueExpr";
		case T_CurrentOfExpr:
			return "CurrentOfExpr";
		case T_SetToDefault:
			return "SetToDefault";
		case T_TableFunc:
			return "TableFunc";
		case T_TableSampleClause:
			return "TableSampleClause";
		default:
			return NULL;
	}
}

static bool
qs_expr_walker(Node *node, void *context)
{
	QsWalk	   *w = (QsWalk *) context;

	if (node == NULL)
		return false;
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			qs_walk_query((Query *) node, w);
			return false;

			/* containers with no semantics of their own */
		case T_List:
		case T_TargetEntry:
		case T_FromExpr:
		case T_RangeTblRef:
		case T_CommonTableExpr:
		case T_CaseWhen:
		case T_WindowClause:
		case T_RangeTblFunction:
		case T_SortGroupClause:
			break;

		case T_Var:
			{
				Var		   *v = (Var *) node;

				qs_check_type(w, v->vartype, "column");
				break;
			}
		case T_Const:
			qs_check_type(w, ((Const *) node)->consttype, "constant");
			qs_check_const(w, (Const *) node);
			break;
		case T_CaseTestExpr:
			qs_check_type(w, ((CaseTestExpr *) node)->typeId, "CASE operand");
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				qs_check_type(w, p->paramtype, "parameter");
				if (p->paramkind == PARAM_EXTERN)
				{
					w->has_extern_param = true;
					w->params = bms_add_member(w->params, p->paramid);
				}
				else if (p->paramkind != PARAM_SUBLINK)
					qs_add(w, "node", "Param(exec)", NULL);
				break;
			}
		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				QsAllowEntry *e = qs_allow_lookup(QA_OPER, op->opno, InvalidOid);

				qs_check_oper(w, op->opno, op->inputcollid);
				if (op->opretset)
					qs_add(w, "feature", "srf", NULL);
				if (e && e->cmp)
				{
					ListCell   *la;

					/*
					 * A comparison does not care what scale its operands were
					 * given, so a bare literal and an implicit widening cast
					 * are exact here and nowhere else.
					 */
					foreach(la, op->args)
					{
						Node	   *arg = (Node *) lfirst(la);

						if (IsA(arg, Const))
							qs_check_type(w, ((Const *) arg)->consttype, "constant");
						else if (IsA(arg, FuncExpr) && ((FuncExpr *) arg)->funcformat == COERCE_IMPLICIT_CAST &&
								 (e = qs_allow_lookup(QA_FUNC, ((FuncExpr *) arg)->funcid, InvalidOid)) != NULL &&
								 e->cmp_only)
							qs_expr_walker((Node *) ((FuncExpr *) arg)->args, w);
						else
							qs_expr_walker(arg, w);
					}
					return false;
				}
				break;
			}
		case T_DistinctExpr:
		case T_NullIfExpr:
			{
				OpExpr	   *op = (OpExpr *) node;

				qs_need(w, "node", qs_node_name(nodeTag(node)));
				qs_check_oper(w, op->opno, op->inputcollid);
				break;
			}
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				Node	   *arr = (Node *) lsecond(sa->args);

				qs_check_oper(w, sa->opno, sa->inputcollid);
				qs_need(w, "feature", sa->useOr ? "in_list" : "all_list");
				if (IsA(arr, ArrayExpr) && !((ArrayExpr *) arr)->multidims)
				{
					/* x IN (a, b, c): the elements matter, the array type does not */
					qs_expr_walker((Node *) linitial(sa->args), w);
					qs_expr_walker((Node *) ((ArrayExpr *) arr)->elements, w);
					return false;
				}
				qs_add(w, "feature", "scalar_array_op", "the array is not a plain list");
				break;
			}
		case T_FuncExpr:
			{
				FuncExpr   *f = (FuncExpr *) node;
				QsAllowEntry *e = qs_allow_lookup(QA_FUNC, f->funcid, InvalidOid);

				qs_check_func(w, QA_FUNC, f->funcid, f->inputcollid);
				if (e && e->cmp_only)
					qs_add(w, "function", format_procedure_extended(f->funcid, FORMAT_PROC_FORCE_QUALIFY),
						   "exact only as the operand of a comparison");
				if (f->funcretset)
					qs_add(w, "feature", "srf", NULL);
				if (f->funcvariadic)
					qs_add(w, "feature", "variadic", NULL);
				break;
			}
		case T_Aggref:
			{
				Aggref	   *a = (Aggref *) node;

				qs_check_func(w, QA_AGG, a->aggfnoid, a->inputcollid);
				if (a->aggkind != AGGKIND_NORMAL)
					qs_add(w, "feature", "ordered_set_agg", NULL);
				if (a->aggvariadic)
					qs_add(w, "feature", "variadic", NULL);
				if (a->aggfilter)
					qs_need(w, "feature", "agg_filter");
				if (a->aggdistinct)
				{
					qs_need(w, "feature", "agg_distinct");
					qs_check_sortgroup(w, a->aggdistinct, a->args, false, true);
				}
				if (a->aggorder)
				{
					qs_need(w, "feature", "agg_order_by");
					qs_check_sortgroup(w, a->aggorder, a->args, true, false);
				}
				if (a->agglevelsup > 0)
					qs_need(w, "feature", "outer_aggregate");
				break;
			}
		case T_WindowFunc:
			{
				WindowFunc *wf = (WindowFunc *) node;

				qs_check_func(w, QA_WINDOW, wf->winfnoid, wf->inputcollid);
				if (wf->aggfilter)
					qs_need(w, "feature", "agg_filter");
				break;
			}
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;
				Oid			src = exprType((Node *) r->arg);

				if (src != r->resulttype && !qs_allow_lookup(QA_CAST, src, r->resulttype))
					qs_add(w, "cast", psprintf("%s->%s", qs_type_name(src), qs_type_name(r->resulttype)), "binary coercion");
				break;
			}
		case T_CoerceViaIO:
			{
				CoerceViaIO *c = (CoerceViaIO *) node;
				Oid			src = exprType((Node *) c->arg);
				QsAllowEntry *e = qs_allow_lookup(QA_CAST, src, c->resulttype);

				if (!e || !e->io)
					qs_add(w, "cast", psprintf("%s->%s", qs_type_name(src), qs_type_name(c->resulttype)), "I/O conversion");
				break;
			}
		case T_SubLink:
			{
				SubLink    *s = (SubLink *) node;

				qs_need(w, "feature",
						s->subLinkType == EXISTS_SUBLINK ? "sublink_exists" :
						s->subLinkType == ANY_SUBLINK ? "sublink_any" :
						s->subLinkType == ALL_SUBLINK ? "sublink_all" :
						s->subLinkType == EXPR_SUBLINK ? "sublink_expr" : "sublink_other");
				break;
			}
		case T_JoinExpr:
			{
				JoinExpr   *j = (JoinExpr *) node;

				qs_need(w, "feature",
						j->jointype == JOIN_INNER ? "join_inner" :
						j->jointype == JOIN_LEFT ? "join_left" :
						j->jointype == JOIN_RIGHT ? "join_right" :
						j->jointype == JOIN_FULL ? "join_full" : "join_other");
				break;
			}
		case T_SetOperationStmt:
			{
				SetOperationStmt *s = (SetOperationStmt *) node;
				ListCell   *lg,
						   *lcoll;

				qs_need(w, "feature",
						s->op == SETOP_UNION ? (s->all ? "union_all" : "union") :
						s->op == SETOP_INTERSECT ? (s->all ? "intersect_all" : "intersect") :
						(s->all ? "except_all" : "except"));
				forboth(lg, s->groupClauses, lcoll, s->colCollations)
				{
					SortGroupClause *sgc = (SortGroupClause *) lfirst(lg);

					if (IsA(sgc, SortGroupClause) && OidIsValid(sgc->eqop))
						qs_check_oper(w, sgc->eqop, lfirst_oid(lcoll));
				}
				forboth(lg, s->colTypes, lcoll, s->colTypmods)
				{
					qs_check_type(w, lfirst_oid(lg), "set operation column");
					qs_check_merge(w, lfirst_oid(lg), lfirst_int(lcoll), "set operation");
				}
				break;
			}
		default:
			{
				const char *name = qs_node_name(nodeTag(node));

				if (IsA(node, CaseExpr) || IsA(node, CoalesceExpr) || IsA(node, MinMaxExpr))
					qs_check_merge(w, exprType(node), exprTypmod(node), name);
				if (name)
					qs_need(w, "node", name);
				else
					qs_add(w, "node", psprintf("tag%d", (int) nodeTag(node)), "node type this version does not know");
				break;
			}
	}
	return expression_tree_walker(node, qs_expr_walker, context);
}

static void
qs_walk_rte(Query *q, RangeTblEntry *rte, QsWalk *w)
{
	switch (rte->rtekind)
	{
		case RTE_RELATION:
			{
				QsMirror   *m = qs_mirror_lookup(rte->relid);
				char	   *relname;

				if (m == NULL || !m->usable)
				{
					relname = (m && m->heapname) ? m->heapname : get_rel_name(rte->relid);
					w->n_unmirrored++;
					qs_add(w, "relation", relname ? relname : "?",
						   m ? (m->reason ? m->reason : "unusable") : "not registered in quicksilver.mirrors");
					break;
				}
				w->mirrors = list_append_unique_ptr(w->mirrors, m);
				if (rte->relkind != RELKIND_RELATION)
					qs_add(w, "relation", m->heapname, "not a plain table");
				/* relhassubclass is only a hint that outlives the last child */
				if (rte->inh && has_subclass(rte->relid) &&
					find_inheritance_children(rte->relid, NoLock) != NIL)
					qs_add(w, "relation", m->heapname, "has inheritance children");
				if (m->rls || rte->securityQuals != NIL)
					qs_add(w, "relation", m->heapname, "row-level security is enabled");
				if (rte->tablesample)
					qs_add(w, "feature", "tablesample", NULL);
				if (rte->perminfoindex > 0)
				{
					RTEPermissionInfo *pi = getRTEPermissionInfo(q->rteperminfos, rte);
					int			col = -1;

					while ((col = bms_next_member(pi->selectedCols, col)) >= 0)
					{
						AttrNumber	attno = col + FirstLowInvalidHeapAttributeNumber;

						if (attno == InvalidAttrNumber)
							qs_need(w, "feature", "whole_row");
						else if (attno < 0)
							qs_add(w, "feature", "system_column", NULL);
						else if (attno > m->natts || !m->col_ok[attno - 1])
							qs_add(w, "column", psprintf("%s.%s", m->heapname, get_attname(rte->relid, attno, true)),
								   attno <= m->natts ? m->col_why[attno - 1] : "added after the registry was read");
						else
						{
							Oid			typid;
							int32		typmod;
							Oid			coll;
							QsAllowEntry *e;

							get_atttypetypmodcoll(rte->relid, attno, &typid, &typmod, &coll);
							e = qs_allow_lookup(QA_TYPE, typid, InvalidOid);
							if (e && e->need_typmod && typmod < 0)
								qs_add(w, "column", psprintf("%s.%s", m->heapname, get_attname(rte->relid, attno, true)),
									   "declared without precision: the mirror cannot hold it exactly");
						}
					}
				}
				else
					qs_add(w, "relation", m->heapname, "no permission entry");
				break;
			}
		case RTE_SUBQUERY:
			/*
			 * A security_barrier view promises that no predicate of the
			 * caller's is evaluated on a row the view hides. DuckDB makes no
			 * such promise about the statement it is given.
			 */
			if (rte->security_barrier)
				qs_add(w, "feature", "security_barrier", "a security_barrier view: the mirror may evaluate a predicate before the barrier");
			qs_need(w, "feature", "subquery_in_from");
			if (rte->lateral)
				qs_need(w, "feature", "lateral");
			break;
		case RTE_JOIN:
		case RTE_RESULT:
		case RTE_CTE:
			break;
		case RTE_VALUES:
			qs_need(w, "feature", "values");
			break;
		case RTE_FUNCTION:
			qs_add(w, "feature", "function_in_from", NULL);
			break;
		default:
			qs_add(w, "feature", "special_range_table_entry", NULL);
			break;
	}
}

static void
qs_walk_query(Query *q, QsWalk *w)
{
	ListCell   *lc;

	if (q->commandType != CMD_SELECT || q->utilityStmt != NULL)
		qs_add(w, "feature", "not_select", NULL);
	if (q->rowMarks != NIL)
		qs_add(w, "feature", "row_locking", "FOR UPDATE/SHARE");
	if (q->hasModifyingCTE)
		qs_add(w, "feature", "modifying_cte", NULL);
	if (q->cteList != NIL)
		qs_need(w, "feature", q->hasRecursive ? "recursive_cte" : "cte");
	if (q->groupClause != NIL || q->groupingSets != NIL)
	{
		qs_need(w, "feature", "group_by");
		if (q->groupingSets != NIL)
			qs_add(w, "feature", "grouping_sets", NULL);
		qs_check_sortgroup(w, q->groupClause, q->targetList, false, true);
	}
	if (q->havingQual != NULL)
		qs_need(w, "feature", "having");
	if (q->distinctClause != NIL)
	{
		qs_need(w, "feature", q->hasDistinctOn ? "distinct_on" : "distinct");
		qs_check_sortgroup(w, q->distinctClause, q->targetList, false, true);
	}
	if (q->sortClause != NIL)
	{
		qs_need(w, "feature", "order_by");
		qs_check_sortgroup(w, q->sortClause, q->targetList, true, false);
	}
	if (q->limitCount != NULL)
	{
		qs_need(w, "feature", "limit");
		if (q->limitOption == LIMIT_OPTION_WITH_TIES)
			qs_need(w, "feature", "with_ties");
	}
	if (q->limitOffset != NULL)
		qs_need(w, "feature", "offset");
	if ((q->limitCount != NULL || q->limitOffset != NULL) && q->sortClause == NIL)
		qs_need(w, "feature", "limit_without_order_by");
	if (q->hasTargetSRFs)
		qs_add(w, "feature", "srf", NULL);
	if (q->windowClause != NIL || q->hasWindowFuncs)
	{
		qs_need(w, "feature", "window");
		foreach(lc, q->windowClause)
		{
			WindowClause *wc = lfirst_node(WindowClause, lc);

			qs_check_sortgroup(w, wc->partitionClause, q->targetList, false, true);
			qs_check_sortgroup(w, wc->orderClause, q->targetList, true, true);
			if (wc->frameOptions != FRAMEOPTION_DEFAULTS)
				qs_need(w, "feature", "window_frame");
		}
	}
	foreach(lc, q->rtable)
		qs_walk_rte(q, lfirst_node(RangeTblEntry, lc), w);

	/* every expression, every sublink, every subquery and CTE below this level */
	query_tree_walker(q, qs_expr_walker, w, 0);
}

/* The whole statement. Returns the walk; safe iff constructs == NIL. */
static void
qs_analyse(Query *q, QsWalk *w)
{
	ListCell   *lc;

	MemoryContext old = CurrentMemoryContext;

	memset(w, 0, sizeof(*w));

	/*
	 * The walk only reads the tree and the catalog caches. If it trips over
	 * something anyway, that is one more reason to stay on the heap, not a
	 * reason to fail the user's statement.
	 */
	PG_TRY();
	{
		qs_walk_query(q, w);
		foreach(lc, q->targetList)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (!tle->resjunk)
				qs_check_type(w, exprType((Node *) tle->expr), "output column");
		}
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		MemoryContextSwitchTo(old);
		ed = CopyErrorData();
		if (qs_must_rethrow(ed->sqlerrcode))
			PG_RE_THROW();
		FlushErrorState();
		qs_add(w, "node", "walk_error", ed->message);
	}
	PG_END_TRY();
}

/* ================================================================ decisions */

static void
qs_record(uint64 queryid, bool mirror, QsReason reason, const char *detail, List *constructs,
		  double cost, double rows)
{
	bool		at_execution = (cost == 0 && rows == 0 && !mirror && qs_in_begin);

	if (qs_nesting > 0 && !at_execution)
	{
		/* a statement inside a function: counted, but it is not "the last statement" */
		if (qs_shared)
			pg_atomic_fetch_add_u64(&qs_shared->reasons[reason], 1);
		return;
	}
	qs_last.valid = true;
	qs_last.fresh = true;
	qs_last.mirror = mirror;
	qs_last.reason = reason;
	qs_last.queryid = queryid;
	qs_last.cost = cost;
	qs_last.rows = rows;
	strlcpy(qs_last.detail, detail ? detail : "", sizeof(qs_last.detail));
	if (qs_last.constructs.data == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

		initStringInfo(&qs_last.constructs);
		MemoryContextSwitchTo(old);
	}
	resetStringInfo(&qs_last.constructs);
	qs_constructs_json(&qs_last.constructs, constructs);

	if (qs_log_decisions)
		ereport(LOG,
				(errmsg("quicksilver: %s: %s%s%s constructs=%s", mirror ? "mirror" : "heap",
						qs_reason_names[reason], detail && detail[0] ? ": " : "", detail ? detail : "",
						qs_last.constructs.data),
				 errhidestmt(false)));

	if (qs_shared)
	{
		QsStatKey	key;
		QsStatEntry *e;
		bool		found = true;

		memset(&key, 0, sizeof(key));
		key.dbid = MyDatabaseId;
		key.queryid = queryid;
		pg_atomic_fetch_add_u64(&qs_shared->reasons[reason], 1);

		/* the common case touches an existing entry under a shared lock */
		LWLockAcquire(qs_shared->lock, LW_SHARED);
		e = hash_search(qs_stat_hash, &key, HASH_FIND, NULL);
		if (e == NULL)
		{
			LWLockRelease(qs_shared->lock);
			LWLockAcquire(qs_shared->lock, LW_EXCLUSIVE);
			e = hash_search(qs_stat_hash, &key,
							hash_get_num_entries(qs_stat_hash) < qs_stats_max ? HASH_ENTER : HASH_FIND, &found);
			if (e && !found)
			{
				pg_atomic_init_u64(&e->n_mirror, 0);
				pg_atomic_init_u64(&e->n_heap, 0);
				pg_atomic_init_u64(&e->n_fallback, 0);
			}
		}
		if (e)
		{
			if (reason == QR_FALLBACK)
				pg_atomic_fetch_add_u64(&e->n_fallback, 1);
			if (at_execution)
			{
				/* it was counted as routed when it was planned */
				if (pg_atomic_read_u64(&e->n_mirror) > 0)
					pg_atomic_fetch_sub_u64(&e->n_mirror, 1);
				pg_atomic_fetch_add_u64(&e->n_heap, 1);
			}
			else if (mirror)
				pg_atomic_fetch_add_u64(&e->n_mirror, 1);
			else
				pg_atomic_fetch_add_u64(&e->n_heap, 1);
			e->last_reason = reason;
			e->last_mirror = mirror;
			strlcpy(e->last_detail, qs_last.detail, sizeof(e->last_detail));
		}
		else
			pg_atomic_fetch_add_u64(&qs_shared->dropped, 1);
		LWLockRelease(qs_shared->lock);
	}
}

static QsDecisionCache *
qs_cache_get(uint64 queryid)
{
	QsDecisionCache *c;

	if (queryid == 0 || qs_decision_hash == NULL)
		return NULL;
	c = hash_search(qs_decision_hash, &queryid, HASH_FIND, NULL);
	if (c == NULL)
		return NULL;
	if (c->generation != qs_generation ||
		(c->expires != 0 && c->expires < GetCurrentTimestamp()))
	{
		hash_search(qs_decision_hash, &queryid, HASH_REMOVE, NULL);
		return NULL;
	}
	return c;
}

static QsDecisionCache *
qs_cache_put(uint64 queryid)
{
	QsDecisionCache *c;
	bool		found;

	if (queryid == 0 || qs_decision_hash == NULL)
		return NULL;
	if (hash_get_num_entries(qs_decision_hash) > 4096)
	{
		HASH_SEQ_STATUS seq;

		hash_seq_init(&seq, qs_decision_hash);
		while ((c = hash_seq_search(&seq)) != NULL)
			hash_search(qs_decision_hash, &c->queryid, HASH_REMOVE, NULL);
	}
	c = hash_search(qs_decision_hash, &queryid, HASH_ENTER, &found);
	if (!found || c->generation != qs_generation)
	{
		c->generation = qs_generation;
		c->negative = false;
		c->no_params = false;
		c->expires = 0;
		c->detail[0] = '\0';
	}
	return c;
}

/* ttl > 0: for that many seconds; ttl < 0: until anything changes; 0: not at all */
static void
qs_cache_negative(uint64 queryid, QsReason reason, const char *detail, int ttl)
{
	QsDecisionCache *c;

	if (ttl == 0 || (c = qs_cache_put(queryid)) == NULL)
		return;
	c->negative = true;
	c->reason = reason;
	c->expires = ttl > 0 ? TimestampTzPlusMilliseconds(GetCurrentTimestamp(), (int64) ttl * 1000) : 0;
	strlcpy(c->detail, detail ? detail : "", sizeof(c->detail));
}

/* ---------------------------------------------------------------- the cost gate */

typedef struct QsCost
{
	PlannedStmt *stmt;
	double		rows;			/* estimated rows read from mirrored relations */
	double		max_scan_rows;
} QsCost;

static void
qs_cost_walk(Plan *plan, double fraction, double loops, QsCost *c)
{
	ListCell   *lc;
	double		childfrac = fraction;

	if (plan == NULL)
		return;
	check_stack_depth();
	switch (nodeTag(plan))
	{
		case T_SeqScan:
		case T_SampleScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_TidRangeScan:
			{
				Index		rti = ((Scan *) plan)->scanrelid;

				if (rti > 0 && rti <= list_length(c->stmt->rtable))
				{
					RangeTblEntry *rte = rt_fetch(rti, c->stmt->rtable);

					if (rte->rtekind == RTE_RELATION && qs_mirror_lookup(rte->relid))
					{
						double		r = plan->plan_rows * fraction * loops;

						c->rows += r;
						c->max_scan_rows = Max(c->max_scan_rows, plan->plan_rows);
					}
				}
				return;			/* a bitmap's index scans are the same rows */
			}
		case T_Limit:

			/*
			 * A scan under a Limit is labelled with the whole table but
			 * stops early, unless something between them has to read all of
			 * its input first.
			 */
			if (plan->lefttree && plan->lefttree->plan_rows > 0)
				childfrac = fraction * Min(1.0, plan->plan_rows / plan->lefttree->plan_rows);
			break;
		case T_Sort:
		case T_IncrementalSort:
		case T_Agg:
		case T_Hash:
		case T_Material:
		case T_WindowAgg:
		case T_SetOp:
		case T_Unique:
		case T_Group:
		case T_Memoize:
			childfrac = 1.0;
			break;
		case T_Append:
			foreach(lc, ((Append *) plan)->appendplans)
				qs_cost_walk((Plan *) lfirst(lc), childfrac, loops, c);
			break;
		case T_MergeAppend:
			foreach(lc, ((MergeAppend *) plan)->mergeplans)
				qs_cost_walk((Plan *) lfirst(lc), childfrac, loops, c);
			break;
		case T_SubqueryScan:
			qs_cost_walk(((SubqueryScan *) plan)->subplan, childfrac, loops, c);
			break;
		case T_CustomScan:
			foreach(lc, ((CustomScan *) plan)->custom_plans)
				qs_cost_walk((Plan *) lfirst(lc), childfrac, loops, c);
			break;
		default:
			break;
	}
	if (IsA(plan, NestLoop))
	{
		qs_cost_walk(plan->lefttree, childfrac, loops, c);
		qs_cost_walk(plan->righttree, 1.0, loops * Max(1.0, plan->lefttree ? plan->lefttree->plan_rows * childfrac : 1.0), c);
	}
	else
	{
		qs_cost_walk(plan->lefttree, childfrac, loops, c);
		qs_cost_walk(plan->righttree, IsA(plan, HashJoin) ? 1.0 : childfrac, loops, c);
	}
}

static void
qs_cost(PlannedStmt *stmt, QsCost *c)
{
	ListCell   *lc;

	c->stmt = stmt;
	c->rows = 0;
	c->max_scan_rows = 0;
	qs_cost_walk(stmt->planTree, 1.0, 1.0, c);
	foreach(lc, stmt->subplans)
		qs_cost_walk((Plan *) lfirst(lc), 1.0, 1.0, c);
}

/* ---------------------------------------------------------------- freshness */

static XLogRecPtr
qs_reported_lsn(Oid relid, bool *blocked)
{
	QsLsnKey	key;
	QsLsnEntry *e;
	XLogRecPtr	lsn = InvalidXLogRecPtr;

	*blocked = false;
	if (!qs_shared)
		return lsn;
	memset(&key, 0, sizeof(key));
	key.dbid = MyDatabaseId;
	key.relid = relid;
	LWLockAcquire(qs_shared->lock, LW_SHARED);
	e = hash_search(qs_lsn_hash, &key, HASH_FIND, NULL);
	if (e)
	{
		lsn = e->lsn;
		/* 0/0 was reported: whoever writes the mirror on this node says not to read it */
		*blocked = (lsn == InvalidXLogRecPtr);
	}
	LWLockRelease(qs_shared->lock);
	return lsn;
}

static XLogRecPtr
qs_current_lsn(void)
{
	/*
	 * What a statement starting now can see on this node. On a standby that is
	 * what has been replayed. On a primary every commit a snapshot can see has
	 * its commit record inserted (an asynchronous commit need not be flushed),
	 * so the insert position bounds it.
	 */
	return RecoveryInProgress() ? GetXLogReplayRecPtr(NULL) : GetXLogInsertRecPtr();
}

static XLogRecPtr
qs_applied_lsn(QsMirror *m)
{
	bool		blocked;
	XLogRecPtr	r;

	if (qs_xact_pinned)
	{
		int			i;

		for (i = 0; i < qs_npins; i++)
			if (qs_pins[i].heap == m->heap)
				return qs_pins[i].lsn;
		return InvalidXLogRecPtr;	/* registered after this transaction's snapshot was fixed */
	}
	r = qs_reported_lsn(m->heap, &blocked);

	/*
	 * The registry's column is the same on every node and each node has its
	 * own mirror: a report of 0/0 here (halted, being rebuilt, not published)
	 * is this node's answer and the column does not overrule it.
	 */
	if (blocked)
		return InvalidXLogRecPtr;
	return Max(r, m->table_lsn);
}

static bool
qs_fresh(QsMirror *m, XLogRecPtr now, char *why, size_t whylen)
{
	XLogRecPtr	applied = qs_applied_lsn(m);
	int64		bound = m->max_lag >= 0 ? m->max_lag : (int64) qs_max_lag_kb * 1024;

	if (applied == InvalidXLogRecPtr)
	{
		bool		blocked;

		(void) qs_reported_lsn(m->heap, &blocked);
		if (qs_xact_pinned && !blocked && qs_reported_lsn(m->heap, &blocked) != InvalidXLogRecPtr)
			snprintf(why, whylen, "%s: nothing was reported for it when this transaction's lake snapshot was fixed", m->heapname);
		else
			snprintf(why, whylen, blocked ? "%s: its mirror on this node is reported unreadable (halted, rebuilding or not published)"
				 : "%s: no applied LSN has been reported", m->heapname);
		return false;
	}
	if (qs_min_applied_lsn != InvalidXLogRecPtr && applied < qs_min_applied_lsn)
	{
		snprintf(why, whylen, "%s: mirror is at %X/%X, quicksilver.min_applied_lsn asks for %X/%X",
				 m->heapname, LSN_FORMAT_ARGS(applied), LSN_FORMAT_ARGS(qs_min_applied_lsn));
		return false;
	}
	if (now > applied && (int64) (now - applied) > bound)
	{
		if (bound == 0)
			snprintf(why, whylen, "%s: mirror is at %X/%X, this node is at %X/%X (" INT64_FORMAT " WAL bytes behind; quicksilver.max_lag = 0 routes only a mirror that has caught up)",
					 m->heapname, LSN_FORMAT_ARGS(applied), LSN_FORMAT_ARGS(now), (int64) (now - applied));
		else
			snprintf(why, whylen, "%s: mirror is " INT64_FORMAT " WAL bytes behind, over " INT64_FORMAT,
					 m->heapname, (int64) (now - applied), bound);
		return false;
	}
	return true;
}

/*
 * Fix what this transaction believes about every mirror. Called with the
 * registry current, immediately before DuckDB is first given anything to do
 * in the transaction.
 */
static void
qs_pin(void)
{
	HASH_SEQ_STATUS seq;
	QsMirror   *m;
	int			n = 0;

	if (qs_xact_pinned || qs_mirror_hash == NULL)
		return;
	if (hash_get_num_entries(qs_mirror_hash) > qs_pins_cap)
	{
		qs_pins_cap = hash_get_num_entries(qs_mirror_hash) + 16;
		if (qs_pins)
			pfree(qs_pins);
		qs_pins = MemoryContextAlloc(TopMemoryContext, sizeof(QsPin) * qs_pins_cap);
	}
	hash_seq_init(&seq, qs_mirror_hash);
	while ((m = hash_seq_search(&seq)) != NULL)
	{
		if (n >= qs_pins_cap)
		{
			hash_seq_term(&seq);
			break;
		}
		qs_pins[n].heap = m->heap;
		qs_pins[n].lsn = qs_applied_lsn(m);	/* live: not pinned yet */
		n++;
	}
	qs_npins = n;
	qs_xact_pinned = true;
}

static void
qs_xact_cb(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
			qs_xact_pinned = false;
			qs_npins = 0;
			qs_gated_plan = NULL;
			qs_gated_lxid = InvalidLocalTransactionId;
			qs_duck_foreign = false;
			if (event == XACT_EVENT_ABORT)
			{
				/* DuckDB rolled back with us: an attachment made in this transaction is gone */
				if (qs_attach_open)
					qs_lakes_attached = false;
				/* and a mirror node that was never ended has left its result open */
				if (qs_mirror_live > 0)
					qs_duck_dirty = true;
			}
			qs_attach_open = false;
			qs_mirror_live = 0;
			break;
		default:
			break;
	}
}

/*
 * How long a statement may wait for its mirror, in milliseconds.
 *
 * Waiting is worth something only in proportion to what routing saves, and
 * what routing saves is bounded by what the heap plan would have taken. So
 * the wait is bounded by a fraction of that (quicksilver.freshness_wait_fraction
 * of the plan's cost, turned into time by quicksilver.cost_unit_ms, which is
 * deliberately low: an under-estimate of the heap waits less) and by an
 * absolute cap (quicksilver.freshness_wait_max). A statement that is cheap
 * on the heap does not wait; one that would take seconds waits long enough
 * for a mirror that is a few hundred milliseconds behind.
 * quicksilver.freshness_wait is a floor, for a session that wants a wait
 * whatever the cost.
 */
static int
qs_wait_budget(double cost)
{
	double		ms = (double) qs_fresh_wait_ms;

	if (qs_wait_fraction > 0 && qs_wait_max_ms > 0 && cost > 0)
	{
		double		est = cost * qs_cost_unit_ms * qs_wait_fraction;

		if (est > (double) qs_wait_max_ms)
			est = (double) qs_wait_max_ms;
		if (est > ms)
			ms = est;
	}
	return (int) ms;
}

static double qs_waited_ms = 0;	/* by the last qs_gate */

/*
 * Is recovery on this standby standing still because of somebody's query?
 *
 * The startup process says what it is waiting for like any other process:
 * a heavyweight lock (an ACCESS EXCLUSIVE lock taken on the primary, against
 * the ACCESS SHARE lock a reader holds), a buffer pin, or the end of the
 * grace period it gives the queries whose snapshots stop it from replaying a
 * cleanup record. Its PGPROC is found once (through the backend status
 * array, which knows which process is the startup process) and looked at
 * without a lock: a stale read costs one more turn of the loop.
 */
static bool
qs_recovery_blocked(void)
{
	static PGPROC *startup = NULL;
	static int	startup_pid = 0;
	uint32		w;

	if (startup == NULL || startup->pid != startup_pid)
	{
		int			n = pgstat_fetch_stat_numbackends();
		int			i;

		startup = NULL;
		for (i = 1; i <= n; i++)
		{
			LocalPgBackendStatus *be = pgstat_get_local_beentry_by_index(i);

			if (be && be->backendStatus.st_backendType == B_STARTUP)
			{
				startup_pid = be->backendStatus.st_procpid;
				startup = AuxiliaryPidGetProc(startup_pid);
				break;
			}
		}
		pgstat_clear_backend_activity_snapshot();
		if (startup == NULL)
			return false;
	}
	w = startup->wait_event_info;
	return (w & 0xFF000000) == PG_WAIT_LOCK || (w & 0xFF000000) == PG_WAIT_BUFFERPIN ||
		w == WAIT_EVENT_RECOVERY_CONFLICT_SNAPSHOT || w == WAIT_EVENT_RECOVERY_CONFLICT_TABLESPACE;
}

/*
 * Are these mirrors (heap OIDs) fresh enough for a statement that can see up
 * to `now` on this node? Returns -1 if so, else the reason with `why` filled.
 *
 * A mirror that is behind is given qs_wait_budget(cost) to catch up, but only
 * while the transaction's lake snapshot is not fixed yet: after that, what
 * the sidecar reports is no longer about what would be read. `reload` is for
 * a caller that holds no pointer into the registry cache.
 *
 * The wait sleeps on a condition variable that quicksilver.report_applied_lsn
 * broadcasts, so it ends with the report that satisfies it and reads the
 * newest position there is; it is interruptible (cancel, statement_timeout,
 * a recovery conflict all end it).
 *
 * It is made while the statement holds what a statement holds: its snapshot
 * and its relation locks. On a standby both can stand in the way of replay.
 * Two things bound that. The budget is a fraction of what the statement would
 * have held them for on the heap anyway. And the wait is given up as soon as
 * the startup process is seen waiting for a lock, a buffer pin or a
 * conflicting snapshot: whoever is in its way, this statement stops being a
 * candidate at once and goes on to the heap, where it ends like any other.
 */
static int
qs_gate(List *heaps, XLogRecPtr now, bool reload, double cost, char *why, size_t whylen)
{
	static uint32 wait_event = 0;
	TimestampTz start = 0;
	int			budget = -1;
	bool		sleeping = false;
	int			state;

	qs_waited_ms = 0;
	for (;;)
	{
		ListCell   *lc;
		bool		can_wait = !qs_xact_pinned;
		TimestampTz ts;
		long		elapsed;

		state = -1;
		if (reload)
			qs_ensure_loaded();
		foreach(lc, heaps)
		{
			QsMirror   *m = qs_mirror_lookup(lfirst_oid(lc));

			if (m == NULL || !m->usable)
			{
				state = QR_NOT_MIRRORED;
				strlcpy(why, "the mirror is no longer registered", whylen);
				can_wait = false;
			}
			else if (!qs_fresh(m, now, why, whylen))
			{
				state = QR_STALE;
				/* nobody is going to report an LSN for it in the next few milliseconds */
				if (qs_applied_lsn(m) == InvalidXLogRecPtr)
					can_wait = false;
			}
			if (state >= 0)
				break;
		}
		if (state < 0 || !can_wait || qs_shared == NULL)
			break;
		ts = GetCurrentTimestamp();
		if (budget < 0)
		{
			budget = qs_wait_budget(cost);
			if (budget <= 0)
				break;
			if (wait_event == 0)
				wait_event = WaitEventExtensionNew("QuicksilverFreshness");
			start = ts;
			/* registered before the next look, so that no report is missed */
			ConditionVariablePrepareToSleep(&qs_shared->reported);
			sleeping = true;
			continue;
		}
		elapsed = (long) ((ts - start) / 1000);
		qs_waited_ms = (double) (ts - start) / 1000.0;
		if (elapsed >= budget)
			break;
		if (RecoveryInProgress() && qs_recovery_blocked())
		{
			strlcat(why, "; not waiting: recovery on this standby is waiting on a conflict", whylen);
			break;
		}
		(void) ConditionVariableTimedSleep(&qs_shared->reported, Min((long) budget - elapsed, 5L), wait_event);
	}
	if (sleeping)
		ConditionVariableCancelSleep();
	return state;
}

/* ---------------------------------------------------------------- the session's state */

/*
 * States in which the mirror is not asked. Checked when the statement is
 * planned and again when a routed plan starts executing: a parameterless
 * prepared statement keeps its plan, and may be executed later inside a
 * transaction block, a savepoint or a function.
 */
static int
qs_state_blocks(const char **detail)
{
	*detail = NULL;
	if (qs_nesting > 0 || IsParallelWorker())
		return QR_NESTED;
	if (IsSubTransaction())
	{
		*detail = "in a subtransaction (savepoint): pg_duckdb refuses";
		return QR_XACT_BLOCK;
	}

	/*
	 * REPEATABLE READ and SERIALIZABLE read one snapshot for the whole
	 * transaction. The lake has snapshots of its own, taken when the sidecar
	 * publishes, and none of them is the transaction's: a routed read there
	 * could show a commit that the transaction's next heap read does not.
	 * READ COMMITTED takes a new snapshot for every statement, and a routed
	 * statement reads a lake snapshot that has reached everything visible
	 * when the statement started (strict freshness), in a DuckDB transaction
	 * of its own: the same rule, statement by statement.
	 */
	if (IsolationUsesXactSnapshot())
	{
		*detail = "the mirror cannot read the transaction's snapshot";
		return QR_ISOLATION;
	}

	/*
	 * A transaction that has an xid has written, or is about to: the mirror
	 * is built from committed changes and cannot show it its own. Whatever
	 * quicksilver.route_in_transaction says.
	 */
	if (TransactionIdIsValid(GetTopTransactionIdIfAny()))
	{
		*detail = "the transaction has written; the mirror cannot see its uncommitted changes";
		return QR_XACT_BLOCK;
	}
	if (qs_in_block())
	{
		if (!qs_route_in_xact)
		{
			*detail = "quicksilver.route_in_transaction is off";
			return QR_XACT_BLOCK;
		}
		if (qs_duck_foreign)
		{
			*detail = "the transaction has used DuckDB directly: its DuckDB transaction is not the router's to end";
			return QR_XACT_BLOCK;
		}
	}
	return -1;
}

/* ---------------------------------------------------------------- building the mirror's statement */

typedef struct QsSubst
{
	ParamListInfo params;
	bool		failed;
} QsSubst;

static void
qs_lock_query(Query *q)
{
	ListCell   *lc;

	foreach(lc, q->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

		if (rte->rtekind == RTE_RELATION)
			LockRelationOid(rte->relid, rte->rellockmode ? rte->rellockmode : AccessShareLock);
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			if (OidIsValid(rte->relid))
				LockRelationOid(rte->relid, AccessShareLock);
			if (rte->subquery)
				qs_lock_query(rte->subquery);
		}
	}
	foreach(lc, q->cteList)
		qs_lock_query(castNode(Query, lfirst_node(CommonTableExpr, lc)->ctequery));
}

/* every relation a (rewritten) statement names, views it came through included */
static bool
qs_relids_walker(Node *node, void *context)
{
	List	  **relids = (List **) context;

	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		ListCell   *lc;

		foreach(lc, ((Query *) node)->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

			if ((rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY) && OidIsValid(rte->relid))
				*relids = list_append_unique_oid(*relids, rte->relid);
		}
		return query_tree_walker((Query *) node, qs_relids_walker, context, 0);
	}
	return expression_tree_walker(node, qs_relids_walker, context);
}

static bool qs_subst_walker(Node *node, void *context);

static void
qs_subst_query(Query *q, QsSubst *s)
{
	ListCell   *lc;
	List	   *perminfos = NIL;

	foreach(lc, q->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);
		QsMirror   *m;

		if (rte->rtekind == RTE_RELATION)
		{
			m = qs_mirror_lookup(rte->relid);
			if (m == NULL || !m->usable || m->subquery == NULL)
			{
				s->failed = true;
				return;
			}
			rte->rtekind = RTE_SUBQUERY;
			rte->subquery = copyObject(m->subquery);
			rte->security_barrier = false;
			rte->relid = InvalidOid;
			rte->relkind = 0;
			rte->rellockmode = 0;
			rte->perminfoindex = 0;
			rte->inh = false;
			rte->tablesample = NULL;
			rte->lateral = false;
			qs_lock_query(rte->subquery);
			continue;
		}
		if (rte->perminfoindex > 0)
		{
			perminfos = lappend(perminfos, getRTEPermissionInfo(q->rteperminfos, rte));
			rte->perminfoindex = list_length(perminfos);
		}
	}
	q->rteperminfos = perminfos;
	query_tree_walker(q, qs_subst_walker, s, 0);
}

static bool
qs_subst_walker(Node *node, void *context)
{
	QsSubst    *s = (QsSubst *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		qs_subst_query((Query *) node, s);
		return false;
	}
	return expression_tree_walker(node, qs_subst_walker, context);
}

/* A custom plan is planned for these parameter values and no others. */
static Node *
qs_param_mutator(Node *node, void *context)
{
	QsSubst    *s = (QsSubst *) context;

	if (node == NULL)
		return NULL;
	if (IsA(node, Param))
	{
		Param	   *p = (Param *) node;

		if (p->paramkind == PARAM_EXTERN)
		{
			ParamExternData *prm;
			int16		typlen;
			bool		typbyval;

			if (s->params == NULL || s->params->paramFetch != NULL ||
				p->paramid <= 0 || p->paramid > s->params->numParams)
			{
				s->failed = true;
				return node;
			}
			prm = &s->params->params[p->paramid - 1];
			if (!(prm->pflags & PARAM_FLAG_CONST) || prm->ptype != p->paramtype)
			{
				s->failed = true;
				return node;
			}
			get_typlenbyval(p->paramtype, &typlen, &typbyval);
			return (Node *) makeConst(p->paramtype, p->paramtypmod, p->paramcollid, (int) typlen,
									  prm->isnull ? (Datum) 0 : datumCopy(prm->value, typbyval, typlen),
									  prm->isnull, typbyval);
		}
		return node;
	}
	if (IsA(node, Query))
		return (Node *) query_tree_mutator((Query *) node, qs_param_mutator, context, 0);
	return expression_tree_mutator(node, qs_param_mutator, context);
}

/* ---------------------------------------------------------------- the routed plan */

typedef enum
{
	QCONV_COPY,
	QCONV_NUMERIC_INT8,
	QCONV_MIRROR_TYPE,			/* allow_unsafe: the mirror's own type; no heap fallback */
} QsConv;

static CustomScanMethods qs_scan_methods;
static CustomExecMethods qs_exec_methods;

typedef struct QsScanState
{
	CustomScanState css;
	PlanState  *mirror_ps;
	PlanState  *heap_ps;
	Plan	   *mirror_plan;
	Plan	   *heap_plan;
	int			eflags;
	int			side;			/* 0 not started, 1 mirror, 2 heap */
	int			natts;
	AttrNumber *heap_attno;
	int		   *conv;
	uint64		queryid;
	char	   *reason;
	char	   *fallback;
	bool		can_fallback;
	Tuplestorestate *buf;		/* the mirror's rows, held back until it is known to finish */
	TupleTableSlot *buf_slot;
	bool		mirror_failed;	/* the mirror raised an error (as opposed to not being asked) */
	int			heap_reason;
	bool		buf_done;		/* the buffer has been handed out */
	bool		mirror_done;	/* the mirror returned its last row */
	bool		explain_only;	/* EXPLAIN without ANALYZE: the mirror node only describes itself */
	bool		eager;			/* in a transaction block: the mirror's whole answer is taken at start */
	bool		streamed;		/* rows were passed on while the mirror was still running */
	bool		timing;
	uint64		mirror_rows;
	instr_time	mirror_time;
	double		waited_ms;
	char	   *mirror_plan_text;
} QsScanState;

static bool
qs_is_duckdb_scan(Plan *plan)
{
	return plan && IsA(plan, CustomScan) && ((CustomScan *) plan)->methods &&
		strcmp(((CustomScan *) plan)->methods->CustomName, QS_DUCKDB_SCAN_NAME) == 0;
}

static bool
qs_offset_walker(Node *node, void *context)
{
	int			offset = *(int *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		if (v->varno > 0 && !IS_SPECIAL_VARNO(v->varno))
			v->varno += offset;
		if (v->varnosyn > 0)
			v->varnosyn += offset;
		return false;
	}
	return expression_tree_walker(node, qs_offset_walker, context);
}

/*
 * Wrap pg_duckdb's plan and the heap plan in one node. The heap statement's
 * range table and permission entries stay in the result, so the executor
 * checks the heap table's privileges exactly as it would have, and the plan
 * depends on the heap relations for invalidation.
 */
static PlannedStmt *
qs_combine(PlannedStmt *heap, PlannedStmt *mirror, uint64 queryid, const char *reason,
		   bool lenient, char *why, size_t whylen, List *mirror_relids, List *heap_relids)
{
	CustomScan *ms = (CustomScan *) mirror->planTree;
	CustomScan *cs;
	List	   *heap_out = NIL;
	List	   *tlist = NIL,
			   *scan_tlist = NIL,
			   *attnos = NIL,
			   *convs = NIL,
			   *colnames = NIL;
	ListCell   *lh,
			   *lm;
	int			rtoffset = list_length(heap->rtable);
	int			permoffset = list_length(heap->permInfos);
	RangeTblEntry *dummy;
	int			dummy_rti;
	int			i;

	if (ms->scan.scanrelid != 0 || ms->scan.plan.lefttree || ms->scan.plan.righttree ||
		ms->custom_plans != NIL || ms->scan.plan.initPlan != NIL || mirror->subplans != NIL ||
		mirror->paramExecTypes != NIL || mirror->rowMarks != NIL || mirror->resultRelations != NIL)
	{
		snprintf(why, whylen, "pg_duckdb's plan has a shape this version does not know");
		return NULL;
	}
	i = 0;
	foreach(lh, heap->planTree->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lh);

		i++;
		if (!tle->resjunk)
		{
			heap_out = lappend(heap_out, tle);
			attnos = lappend_int(attnos, i);
		}
	}
	foreach(lm, ms->scan.plan.targetlist)
		if (lfirst_node(TargetEntry, lm)->resjunk)
		{
			snprintf(why, whylen, "pg_duckdb's plan has hidden columns");
			return NULL;
		}
	if (list_length(heap_out) != list_length(ms->scan.plan.targetlist))
	{
		snprintf(why, whylen, "the mirror returns %d columns, the heap %d",
				 list_length(ms->scan.plan.targetlist), list_length(heap_out));
		return NULL;
	}

	/* the row-naming entry goes right after the mirror statement's own */
	dummy_rti = rtoffset + list_length(mirror->rtable) + 1;
	i = 0;
	forboth(lh, heap_out, lm, ms->scan.plan.targetlist)
	{
		TargetEntry *h = lfirst_node(TargetEntry, lh);
		TargetEntry *m = lfirst_node(TargetEntry, lm);
		Oid			ht = exprType((Node *) h->expr),
					mt = exprType((Node *) m->expr);
		int32		hm = exprTypmod((Node *) h->expr),
					mm = exprTypmod((Node *) m->expr);
		Oid			hc = exprCollation((Node *) h->expr);
		int			conv = QCONV_COPY;
		Oid			outt = ht;
		int32		outm = hm;

		i++;
		if (ht == mt && (hm == mm || ht == NUMERICOID))
		{
			/*
			 * A numeric is the same datum whatever precision the column
			 * descriptor claims; the heap's descriptor is the one the client
			 * sees.
			 */
		}
		else if (ht == INT8OID && mt == NUMERICOID)
			conv = QCONV_NUMERIC_INT8;
		else if (lenient)
		{
			conv = QCONV_MIRROR_TYPE;
			outt = mt;
			outm = mm;
			hc = exprCollation((Node *) m->expr);
		}
		else
		{
			snprintf(why, whylen, "column %d: heap %s, mirror %s", i,
					 format_type_with_typemod(ht, hm), format_type_with_typemod(mt, mm));
			return NULL;
		}
		convs = lappend_int(convs, conv);
		colnames = lappend(colnames, makeString(pstrdup(h->resname ? h->resname : "?column?")));
		scan_tlist = lappend(scan_tlist,
							 makeTargetEntry((Expr *) makeVar(dummy_rti, i, outt, outm, hc, 0), i,
											 h->resname ? pstrdup(h->resname) : NULL, false));
		tlist = lappend(tlist,
						makeTargetEntry((Expr *) makeVar(INDEX_VAR, i, outt, outm, hc, 0), i,
										h->resname ? pstrdup(h->resname) : NULL, false));
		((TargetEntry *) llast(tlist))->resorigtbl = h->resorigtbl;
		((TargetEntry *) llast(tlist))->resorigcol = h->resorigcol;
	}

	/* the mirror statement's range table goes after the heap's */
	foreach(lm, mirror->rtable)
	{
		RangeTblEntry *rte = lfirst_node(RangeTblEntry, lm);

		if (rte->perminfoindex > 0)
			rte->perminfoindex += permoffset;
	}
	qs_offset_walker((Node *) ms->custom_scan_tlist, &rtoffset);
	qs_offset_walker((Node *) ms->custom_exprs, &rtoffset);
	if (ms->custom_relids)
	{
		Bitmapset  *shifted = NULL;
		int			x = -1;

		while ((x = bms_next_member(ms->custom_relids, x)) >= 0)
			shifted = bms_add_member(shifted, x + rtoffset);
		ms->custom_relids = shifted;
	}
	heap->rtable = list_concat(heap->rtable, mirror->rtable);
	/* a name for each output column, for EXPLAIN VERBOSE */
	dummy = makeNode(RangeTblEntry);
	dummy->rtekind = RTE_RESULT;
	dummy->eref = makeAlias("quicksilver", colnames);
	dummy->alias = makeAlias("quicksilver", NIL);
	dummy->lateral = false;
	dummy->inFromCl = false;
	heap->rtable = lappend(heap->rtable, dummy);
	Assert(list_length(heap->rtable) == dummy_rti);

	heap->permInfos = list_concat(heap->permInfos, mirror->permInfos);
	heap->relationOids = list_concat_unique_oid(heap->relationOids, mirror->relationOids);

	/*
	 * A plan may be kept (a prepared statement without parameters is planned
	 * once). It must then die with anything the decision rested on: the
	 * mirror relations, the registry, the allowlist. The plan cache discards
	 * a plan when a relation in relationOids changes, and the triggers on the
	 * two tables send exactly that signal. And whoever runs the plan must
	 * hold a lock on the mirror relations, as it does on the heap's: an
	 * entry in the range table is what the plan cache locks before reuse.
	 */
	heap->relationOids = list_concat_unique_oid(heap->relationOids, mirror_relids);
	heap->relationOids = list_append_unique_oid(heap->relationOids, qs_mirrors_relid);
	heap->relationOids = list_append_unique_oid(heap->relationOids, qs_allowlist_relid);
	foreach(lm, mirror_relids)
	{
		Oid			relid = lfirst_oid(lm);
		bool		present = false;

		foreach(lh, heap->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lh);

			if (rte->relid == relid && (rte->rtekind == RTE_RELATION || rte->rtekind == RTE_SUBQUERY))
				present = true;
		}
		if (!present)
		{
			RangeTblEntry *lockrte = makeNode(RangeTblEntry);

			lockrte->rtekind = RTE_SUBQUERY;	/* what a flattened view leaves behind */
			lockrte->relid = relid;
			lockrte->relkind = get_rel_relkind(relid);
			lockrte->rellockmode = AccessShareLock;
			lockrte->eref = makeAlias("quicksilver_mirror", NIL);
			heap->rtable = lappend(heap->rtable, lockrte);
		}
	}
	heap->invalItems = list_concat(heap->invalItems, mirror->invalItems);
	heap->dependsOnRole = heap->dependsOnRole || mirror->dependsOnRole;

	cs = makeNode(CustomScan);
	cs->scan.plan.startup_cost = heap->planTree->startup_cost;
	cs->scan.plan.total_cost = heap->planTree->total_cost;	/* the only cost PostgreSQL knows */
	cs->scan.plan.plan_rows = heap->planTree->plan_rows;
	cs->scan.plan.plan_width = heap->planTree->plan_width;
	cs->scan.plan.parallel_aware = false;
	cs->scan.plan.parallel_safe = false;
	cs->scan.plan.targetlist = tlist;
	cs->scan.scanrelid = 0;
	cs->flags = 0;
	cs->custom_plans = list_make2(ms, heap->planTree);
	cs->custom_scan_tlist = scan_tlist;
	cs->custom_private = list_make5(attnos, convs, makeString(pstrdup(reason)),
									makeString(psprintf(UINT64_FORMAT, queryid)), heap_relids);
	cs->methods = &qs_scan_methods;

	heap->planTree = (Plan *) cs;
	heap->parallelModeNeeded = false;
	return heap;
}

static Node *
qs_create_scan_state(CustomScan *cscan)
{
	QsScanState *st = (QsScanState *) newNode(sizeof(QsScanState), T_CustomScanState);

	st->css.methods = &qs_exec_methods;
	return (Node *) st;
}

/* Errors that mean "stop", not "this side cannot answer". */
static bool
qs_must_rethrow(int sqlerrcode)
{
	return sqlerrcode == ERRCODE_QUERY_CANCELED ||
		sqlerrcode == ERRCODE_STATEMENT_TOO_COMPLEX ||
		sqlerrcode == ERRCODE_INSUFFICIENT_PRIVILEGE ||
		ERRCODE_TO_CATEGORY(sqlerrcode) == ERRCODE_TRANSACTION_ROLLBACK ||	/* 40: deadlock, serialization */
		ERRCODE_TO_CATEGORY(sqlerrcode) == ERRCODE_INSUFFICIENT_RESOURCES ||	/* 53: out of memory, disk full */
		ERRCODE_TO_CATEGORY(sqlerrcode) == ERRCODE_OPERATOR_INTERVENTION;	/* 57: cancel, shutdown */
}

/*
 * May an error thrown while the mirror was being planned or run be answered
 * from the heap instead?
 *
 * There is no subtransaction around the mirror (pg_duckdb refuses to start
 * one once its DuckDB transaction is open), so nothing rolls back what the
 * failing code had acquired. The only errors swallowed are therefore the ones
 * where there is nothing to roll back: an error DuckDB raised, which pg_duckdb
 * turns into a PostgreSQL error at its own C++ boundary, after the C++ stack
 * has unwound and released what it held, and which it prefixes with the name
 * of the entry point. DuckDB reads the lake's files; it takes no buffer pin,
 * no relcache reference and no lightweight lock on the way. Anything else is
 * rethrown: an error of PostgreSQL's own (it may have been holding any of
 * those), anything that is not a plain ERROR, a cancel or a shutdown however
 * it is spelled, and the classes a retry on the heap must not hide.
 */
static bool
qs_mirror_error_ok(ErrorData *ed)
{
	if (ed->elevel != ERROR || QueryCancelPending || ProcDiePending)
		return false;
	if (qs_must_rethrow(ed->sqlerrcode))
		return false;
	if (ed->message == NULL)
		return false;
	/* pg_duckdb's refusal at the door (after an EXPLAIN in the same transaction): nothing has been started */
	if (strcmp(ed->message, "DuckDB execution is not supported inside functions") == 0)
		return true;
	if (strncmp(ed->message, "(PGDuckDB/", 10) != 0)
		return false;
	if (strstr(ed->message, "INTERRUPT Error") || strstr(ed->message, "Query cancelled") ||
		strstr(ed->message, "Out of Memory Error") ||
		strstr(ed->message, "INTERNAL Error") || strstr(ed->message, "FATAL Error"))
		return false;
	return true;
}

static void
qs_to_heap(QsScanState *st, QsReason reason, const char *msg)
{
	char	   *line = MemoryContextStrdup(st->css.ss.ps.state->es_query_cxt, msg);

	/* DuckDB's messages go on for lines; the first one says what happened */
	if (strchr(line, '\n'))
		*strchr(line, '\n') = '\0';
	msg = line;
	st->side = 2;
	st->heap_reason = reason;
	qs_in_begin = true;
	st->fallback = line;
	st->buf_done = true;
	if (!IsTransactionBlock() && st->mirror_failed)
		qs_cache_negative(st->queryid, QR_MIRROR_PLAN_FAILED, msg, qs_negative_ttl);
	qs_record(st->queryid, false, reason, msg, NIL, 0, 0);
	qs_in_begin = false;
}

/* One row of the mirror's, as the heap would have typed it. */
static void
qs_convert(QsScanState *st, TupleTableSlot *child, TupleTableSlot *slot, bool from_mirror)
{
	int			i;

	ExecClearTuple(slot);
	slot_getallattrs(child);
	for (i = 0; i < st->natts; i++)
	{
		int			src = from_mirror ? i : st->heap_attno[i] - 1;

		slot->tts_isnull[i] = child->tts_isnull[src];
		slot->tts_values[i] = child->tts_values[src];
		if (from_mirror && st->conv[i] == QCONV_NUMERIC_INT8 && !slot->tts_isnull[i])
			slot->tts_values[i] = DirectFunctionCall1(numeric_int8, slot->tts_values[i]);
	}
	ExecStoreVirtualTuple(slot);
}

/*
 * Every call into pg_duckdb's node is made as the bootstrap superuser (see
 * QsElev) and, under EXPLAIN ANALYZE, with the portal's command tag turned
 * into SELECT: pg_duckdb looks at that tag to decide whether to run the
 * statement or DuckDB's own EXPLAIN ANALYZE of it, which returns no rows and
 * runs it a second time to be described. An explained statement is run the
 * way it would have been run, once, and its rows are counted here.
 */
typedef struct QsCall
{
	QsElev		elev;
	bool		swapped;
} QsCall;

static inline void
qs_call_begin(QsScanState *st, QsCall *c)
{
	c->swapped = !st->explain_only && ActivePortal != NULL && ActivePortal->commandTag == CMDTAG_EXPLAIN;
	if (c->swapped)
		ActivePortal->commandTag = CMDTAG_SELECT;
	qs_elev_begin(&c->elev);
}

static inline void
qs_call_end(QsCall *c)
{
	qs_elev_end(&c->elev);
	if (c->swapped && ActivePortal != NULL)
		ActivePortal->commandTag = CMDTAG_EXPLAIN;
}

/*
 * The mirror has said everything, or is not wanted any more: end its node,
 * and DuckDB's transaction with it. False if DuckDB did not finish the
 * statement (qs_duck_end), which matters to a caller that took "no more rows"
 * for the end of the answer.
 */
static bool
qs_mirror_finish(QsScanState *st)
{
	if (st->mirror_ps)
	{
		QsCall		call;

		qs_call_begin(st, &call);
		ExecEndNode(st->mirror_ps);
		qs_call_end(&call);
		st->mirror_ps = NULL;
		if (qs_mirror_live > 0)
			qs_mirror_live--;
	}
	return qs_duck_end(false);
}

/* The mirror raised an error DuckDB made: forget its node and let the heap answer. */
static void
qs_mirror_failed(QsScanState *st, const char *message)
{
	if (st->buf)
		tuplestore_end(st->buf);
	st->buf = NULL;
	st->mirror_failed = true;
	st->mirror_done = false;
	st->mirror_rows = 0;

	/*
	 * The node threw: it is not ended (ExecEndNode on it would run
	 * pg_duckdb's end-of-scan on a state its own error path has already
	 * cleaned up). What it allocated in PostgreSQL goes with the query's
	 * memory context, as it would on an abort.
	 */
	if (st->mirror_ps && qs_mirror_live > 0)
		qs_mirror_live--;
	st->mirror_ps = NULL;
	(void) qs_duck_end(true);
	qs_to_heap(st, QR_FALLBACK, message);
}

#define QS_CUT_SHORT "the mirror's result ended early: DuckDB failed after it had started returning rows, and pg_duckdb raised no error"

/*
 * Make pg_duckdb open an explicit DuckDB transaction for what follows.
 *
 * It does when it believes the statement is not a top-level one, and it
 * believes that from the moment its planner hook has planned anything for
 * PostgreSQL in this transaction. That is every statement the router has
 * just planned (the heap plan went through that hook), but not a plan kept
 * from an earlier transaction: for that one pg_duckdb would run the mirror's
 * statement in DuckDB's autocommit mode, where a failure leaves nothing
 * behind for qs_duck_end to find. Its hook is given a statement of no
 * consequence to plan. (Sending DuckDB a BEGIN instead is no good: where a
 * transaction is already open it fails, and the failure invalidates it.)
 */
static void
qs_duck_explicit(void)
{
	Query	   *q;

	if (prev_planner_hook == NULL)
		return;
	q = makeNode(Query);
	q->commandType = CMD_SELECT;
	q->querySource = QSRC_ORIGINAL;
	q->canSetTag = true;
	q->targetList = list_make1(makeTargetEntry((Expr *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
																   Int32GetDatum(1), false, true),
											   1, pstrdup("one"), false));
	q->jointree = makeFromExpr(NIL, NULL);
	(void) prev_planner_hook(q, "SELECT 1", 0, NULL);
}

static bool
qs_mirror_start(QsScanState *st, EState *estate)
{
	MemoryContext old = CurrentMemoryContext;
	QsCall		call;
	instr_time	t0,
				t1;

	INSTR_TIME_SET_CURRENT(t0);

	qs_duck_explicit();
	qs_call_begin(st, &call);
	PG_TRY();
	{
		st->mirror_ps = ExecInitNode(st->mirror_plan, estate, st->eflags);
		qs_mirror_live++;
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		qs_call_end(&call);
		MemoryContextSwitchTo(old);
		ed = CopyErrorData();
		if (!st->can_fallback || !qs_mirror_error_ok(ed))
			PG_RE_THROW();
		FlushErrorState();
		st->mirror_ps = NULL;
		qs_lakes_attached = false;
		qs_mirror_failed(st, ed->message);
		return false;
	}
	PG_END_TRY();
	qs_call_end(&call);
	INSTR_TIME_SET_CURRENT(t1);
	INSTR_TIME_ACCUM_DIFF(st->mirror_time, t1, t0);
	return true;
}

/*
 * Take the mirror's rows and hold them back.
 *
 * Nothing has been returned yet, so nothing has reached the client: if the
 * mirror fails anywhere in this stretch (a value it cannot represent, a file
 * that went away, an overflow PostgreSQL would also report) the heap answers
 * the whole statement and the client never knows.
 *
 * Outside a transaction block the stretch ends when the rows no longer fit
 * in work_mem: the rest is streamed, and an error after that point is the
 * client's to see, as an error, with no retry on the heap behind it (the
 * rows already sent cannot be taken back, and sending the heap's as well
 * would send some twice).
 *
 * In a transaction block (`all`) the mirror is read to its end here, however
 * large the answer (the tuplestore goes to a temporary file past work_mem),
 * and its node and DuckDB's transaction are ended before the first row is
 * returned: the client may send anything next (a SAVEPOINT, a write), with a
 * cursor or a suspended portal still open over these rows, and pg_duckdb must
 * be idle when it does.
 */
static void
qs_mirror_drain(QsScanState *st, bool all)
{
	TupleTableSlot *slot = st->css.ss.ss_ScanTupleSlot;
	MemoryContext old = CurrentMemoryContext;
	QsCall		call;
	instr_time	t0,
				t1;

	INSTR_TIME_SET_CURRENT(t0);
	qs_call_begin(st, &call);
	PG_TRY();
	{
		MemoryContext qcxt = MemoryContextSwitchTo(st->css.ss.ps.state->es_query_cxt);

		st->buf = tuplestore_begin_heap(false, false, work_mem);
		tuplestore_set_eflags(st->buf, st->eflags & EXEC_FLAG_REWIND);
		MemoryContextSwitchTo(qcxt);
		for (;;)
		{
			TupleTableSlot *child = ExecProcNode(st->mirror_ps);

			if (TupIsNull(child))
			{
				st->mirror_done = true;
				break;
			}
			st->mirror_rows++;
			qs_convert(st, child, slot, true);
			tuplestore_puttupleslot(st->buf, slot);
			if (!all && (!st->can_fallback || !tuplestore_in_memory(st->buf)))
				break;
			CHECK_FOR_INTERRUPTS();
		}
		st->side = 1;
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		qs_call_end(&call);
		MemoryContextSwitchTo(old);
		ed = CopyErrorData();
		if (!st->can_fallback || !qs_mirror_error_ok(ed))
			PG_RE_THROW();
		FlushErrorState();
		qs_mirror_failed(st, ed->message);
		INSTR_TIME_SET_CURRENT(t1);
		INSTR_TIME_ACCUM_DIFF(st->mirror_time, t1, t0);
		return;
	}
	PG_END_TRY();
	qs_call_end(&call);
	ExecClearTuple(slot);

	/*
	 * "No more rows" is the end of the answer only if DuckDB agrees. Nothing
	 * has been returned yet, so if it does not, the heap answers.
	 */
	if (st->mirror_done && !qs_mirror_finish(st))
	{
		if (!st->can_fallback)
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("%s", QS_CUT_SHORT)));
		qs_mirror_failed(st, QS_CUT_SHORT);
	}
	INSTR_TIME_SET_CURRENT(t1);
	INSTR_TIME_ACCUM_DIFF(st->mirror_time, t1, t0);
}

static void
qs_begin_scan(CustomScanState *node, EState *estate, int eflags)
{
	QsScanState *st = (QsScanState *) node;
	CustomScan *cs = (CustomScan *) node->ss.ps.plan;
	List	   *attnos = (List *) linitial(cs->custom_private);
	List	   *convs = (List *) lsecond(cs->custom_private);
	ListCell   *la,
			   *lc;
	int			i = 0;

	st->can_fallback = qs_fallback;
	st->mirror_plan = (Plan *) linitial(cs->custom_plans);
	st->heap_plan = (Plan *) lsecond(cs->custom_plans);
	st->eflags = eflags;
	st->explain_only = (eflags & EXEC_FLAG_EXPLAIN_ONLY) != 0;
	st->timing = estate->es_instrument != 0;
	INSTR_TIME_SET_ZERO(st->mirror_time);
	st->natts = list_length(attnos);
	st->heap_attno = palloc(sizeof(AttrNumber) * Max(st->natts, 1));
	st->conv = palloc(sizeof(int) * Max(st->natts, 1));
	forboth(la, attnos, lc, convs)
	{
		st->heap_attno[i] = lfirst_int(la);
		st->conv[i] = lfirst_int(lc);
		if (st->conv[i] == QCONV_MIRROR_TYPE)
			st->can_fallback = false;
		i++;
	}
	st->reason = strVal(lthird(cs->custom_private));
	st->queryid = strtou64(strVal(lfourth(cs->custom_private)), NULL, 10);
	st->side = 0;

	/* the heap side is initialised first: it is the one that must always work */
	st->heap_ps = ExecInitNode(st->heap_plan, estate, eflags);
	/* the mirror's node is never shown to EXPLAIN as a child: see qs_explain_scan */
	node->custom_ps = list_make1(st->heap_ps);
	st->buf_slot = MakeSingleTupleTableSlot(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor, &TTSOpsMinimalTuple);

	/*
	 * The plan may be older than this execution. What can have changed
	 * without the plan being thrown away is the session (its role, a
	 * transaction block, a savepoint, the routing mode) and the mirror's
	 * freshness.
	 */
	{
		const char *why = NULL;
		char		stale[QS_DETAIL_LEN];
		int			state = qs_state_blocks(&why);

		if (state >= 0 && why == NULL)
			why = "";
		if (state < 0 && qs_route != QS_ROUTE_AUTO && qs_route != QS_ROUTE_MIRROR)
		{
			state = QR_MODE_HEAP;
			why = "quicksilver.route no longer asks for the mirror";
		}
		if (why == NULL && !qs_mirror_allowed(stale, sizeof(stale)))
		{
			state = QR_UNCONFINED;
			why = stale;
		}
		if (why == NULL)
		{
			/*
			 * Freshness. A plan made a moment ago in this transaction was
			 * checked when it was made, before DuckDB was asked anything
			 * about it. Any other plan (kept from an earlier transaction, or
			 * run a second time in this one) is checked now, against the
			 * position this execution can see: with a wait if the lake has
			 * not been touched in this transaction, and from what was pinned
			 * if it has.
			 */
			bool		gated = qs_gated_plan == (void *) cs && qs_gated_lxid == MyProc->vxid.lxid;

			qs_gated_plan = NULL;
			if (!gated)
			{
				state = qs_gate((List *) list_nth(cs->custom_private, 4), qs_current_lsn(), true,
								cs->scan.plan.total_cost, stale, sizeof(stale));
				if (state >= 0)
					why = stale;
				st->waited_ms = qs_waited_ms;
			}
			else
				st->waited_ms = qs_gated_waited_ms;
		}
		if (why)
		{
			if (!st->can_fallback && !qs_fallback)
				why = NULL;		/* the user asked for the mirror's errors, not the heap */
			else if (!st->can_fallback)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("a plan made with quicksilver.allow_unsafe cannot be answered from the heap"),
						 errdetail("%s", why)));
		}
		if (why)
		{
			qs_to_heap(st, state, why);
			return;
		}
	}
	qs_pin();
	qs_duck_recycle();
	(void) qs_attach_lakes(false);
	st->eager = !st->explain_only && qs_in_block();
	if (!qs_mirror_start(st, estate))
		return;
	if (st->eager)
		qs_mirror_drain(st, true);
}

static TupleTableSlot *
qs_exec_scan(CustomScanState *node)
{
	QsScanState *st = (QsScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TupleTableSlot *child;
	QsCall		call;
	instr_time	t0,
				t1;

	CHECK_FOR_INTERRUPTS();
	if (st->side == 0)
		qs_mirror_drain(st, false);

	if (st->side == 2)
	{
		child = ExecProcNode(st->heap_ps);
		if (TupIsNull(child))
			return ExecClearTuple(slot);
		qs_convert(st, child, slot, false);
		return slot;
	}
	if (!st->buf_done)
	{
		if (tuplestore_gettupleslot(st->buf, true, false, st->buf_slot))
			return st->buf_slot;
		st->buf_done = true;
	}
	if (st->mirror_done)
		return ExecClearTuple(slot);

	/*
	 * Streaming: rows have left the server. An error from here on is not
	 * caught (there is no PG_TRY around this call): it ends the statement as
	 * an error, after the rows already sent and with nothing after them.
	 */
	st->streamed = true;
	if (st->timing)
		INSTR_TIME_SET_CURRENT(t0);
	qs_call_begin(st, &call);
	child = ExecProcNode(st->mirror_ps);
	qs_call_end(&call);
	if (st->timing)
	{
		INSTR_TIME_SET_CURRENT(t1);
		INSTR_TIME_ACCUM_DIFF(st->mirror_time, t1, t0);
	}
	if (TupIsNull(child))
	{
		st->mirror_done = true;

		/*
		 * The stream has ended. If DuckDB did not finish the statement, the
		 * rows the client holds are not the answer, and it must be told.
		 */
		if (!qs_mirror_finish(st))
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("the mirror failed after rows had been returned"),
					 errdetail("DuckDB did not finish the statement, and pg_duckdb ended its result without an error. "
							   "The " UINT64_FORMAT " rows already sent are not the whole answer; nothing was sent twice.",
							   st->mirror_rows),
					 errhint("Run the statement again: SET quicksilver.route = heap answers it from the heap.")));
		return ExecClearTuple(slot);
	}
	st->mirror_rows++;
	qs_convert(st, child, slot, true);
	return slot;
}

static void
qs_end_scan(CustomScanState *node)
{
	QsScanState *st = (QsScanState *) node;

	if (st->buf)
		tuplestore_end(st->buf);
	st->buf = NULL;
	if (st->buf_slot)
		ExecDropSingleTupleTableSlot(st->buf_slot);
	st->buf_slot = NULL;
	if (st->mirror_ps)
		(void) qs_mirror_finish(st);
	if (st->heap_ps)
		ExecEndNode(st->heap_ps);
}

static void
qs_rescan(CustomScanState *node)
{
	QsScanState *st = (QsScanState *) node;

	if (st->side == 2)
		ExecReScan(st->heap_ps);
	else if (st->side == 1)
	{
		if (st->mirror_done && st->buf && (st->eflags & EXEC_FLAG_REWIND))
		{
			/* everything the mirror said is still here */
			tuplestore_rescan(st->buf);
			st->buf_done = false;
		}
		else if (st->mirror_ps)
		{
			QsCall		call;

			if (st->buf)
				tuplestore_end(st->buf);
			st->buf = NULL;
			st->buf_done = false;
			st->mirror_done = false;
			st->mirror_rows = 0;
			st->side = 0;
			qs_call_begin(st, &call);
			ExecReScan(st->mirror_ps);
			qs_call_end(&call);
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("a statement answered from the mirror cannot be scanned again")));
	}
}

/*
 * DuckDB's plan for the mirror's statement, as pg_duckdb describes it. Only
 * under EXPLAIN without ANALYZE, where the mirror node was prepared as an
 * EXPLAIN and running it describes instead of executes. Asked for once.
 */
static void
qs_explain_mirror_plan(QsScanState *st, List *ancestors, ExplainState *es)
{
	if (st->mirror_plan_text == NULL)
	{
		CustomScanState *child = (CustomScanState *) st->mirror_ps;
		ExplainState *tmp = NewExplainState();
		MemoryContext old = CurrentMemoryContext;
		QsCall		call;
		char	   *text;

		qs_call_begin(st, &call);
		PG_TRY();
		{
			child->methods->ExplainCustomScan(child, ancestors, tmp);
		}
		PG_CATCH();
		{
			ErrorData  *ed;

			qs_call_end(&call);
			MemoryContextSwitchTo(old);
			ed = CopyErrorData();
			if (!qs_mirror_error_ok(ed))
				PG_RE_THROW();
			FlushErrorState();
			resetStringInfo(tmp->str);
			appendStringInfo(tmp->str, "(not available: %s)", ed->message);
			memset(&call, 0, sizeof(call));
		}
		PG_END_TRY();
		qs_call_end(&call);
		text = tmp->str->data;
		if (strncmp(text, "DuckDB Execution Plan: ", 23) == 0)
			text += 23;
		while (*text == '\n')
			text++;
		while (strlen(text) > 0 && text[strlen(text) - 1] == '\n')
			text[strlen(text) - 1] = '\0';
		st->mirror_plan_text = MemoryContextStrdup(st->css.ss.ps.state->es_query_cxt, text);
	}
	ExplainPropertyText("Mirror Plan", psprintf("\n%s\n", st->mirror_plan_text), es);
}

/*
 * Which side, why, and what the mirror did. pg_duckdb's node is not shown as
 * a child: its own EXPLAIN output is produced by running the statement (again)
 * under DuckDB's EXPLAIN, as whoever is asking, and after a real execution
 * (EXPLAIN ANALYZE, auto_explain) that is an error or worse.
 */
static void
qs_explain_scan(CustomScanState *node, List *ancestors, ExplainState *es)
{
	QsScanState *st = (QsScanState *) node;

	if (!st->fallback)
	{
		ExplainPropertyText("Quicksilver", "mirror", es);
		if (es->costs || es->verbose)
			ExplainPropertyText("Quicksilver Reason", st->reason, es);
		if (st->explain_only)
		{
			if (st->mirror_ps && (es->costs || es->verbose))
				qs_explain_mirror_plan(st, ancestors, es);
		}
		else if (es->analyze)
		{
			ExplainPropertyUInteger("Mirror Rows", NULL, st->mirror_rows, es);
			ExplainPropertyText("Mirror Result",
								st->eager ? "taken whole at start (transaction block)" :
								st->streamed ? "streamed once past work_mem" : "held back until complete", es);
			if (es->timing)
			{
				ExplainPropertyFloat("Mirror Time", "ms", INSTR_TIME_GET_MILLISEC(st->mirror_time), 3, es);
				if (st->waited_ms > 0)
					ExplainPropertyFloat("Mirror Freshness Wait", "ms", st->waited_ms, 3, es);
			}
		}
	}
	else
	{
		ExplainPropertyText("Quicksilver", psprintf("heap (%s)", qs_reason_names[st->heap_reason]), es);
		if ((es->costs || es->verbose || st->mirror_failed) && st->fallback[0])
			ExplainPropertyText("Quicksilver Detail", st->fallback, es);
	}
}

/* ---------------------------------------------------------------- the decision */

typedef struct QsRels
{
	int			mirrored;
	int			other;
	List	   *mirror_oids;
} QsRels;

/* Which relations does the statement read, at any depth? Nothing else. */
static bool
qs_rels_walker(Node *node, void *context)
{
	QsRels	   *r = (QsRels *) context;

	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		ListCell   *lc;

		foreach(lc, ((Query *) node)->rtable)
		{
			RangeTblEntry *rte = lfirst_node(RangeTblEntry, lc);

			if (rte->rtekind == RTE_RELATION)
			{
				QsMirror   *m = qs_mirror_lookup(rte->relid);

				if (m)
				{
					r->mirrored++;
					r->mirror_oids = list_append_unique_oid(r->mirror_oids, m->mirror);
				}
				else
					r->other++;
			}
		}
		return query_tree_walker((Query *) node, qs_rels_walker, context, 0);
	}
	return expression_tree_walker(node, qs_rels_walker, context);
}

static PlannedStmt *
qs_decide(Query *orig, PlannedStmt *heap, const char *query_string, int cursorOptions,
		  ParamListInfo boundParams, bool any_other)
{
	uint64		queryid = orig->queryId;
	QsCost		cost;
	QsWalk		w;
	QsDecisionCache *ce;
	bool		cheap;
	bool		lenient = false;
	char		detail[QS_DETAIL_LEN];
	ListCell   *lc;
	XLogRecPtr	now;
	Query	   *mq;
	QsSubst		subst;
	PlannedStmt *mplan = NULL;
	PlannedStmt *result;
	uint64		generation;
	MemoryContext old;
	int			state;
	const char *state_detail;
	QsElev		elev;
	List	   *mirror_relids = NIL;
	List	   *heap_relids = NIL;

	detail[0] = '\0';
	qs_cost(heap, &cost);

#define QS_HEAP(reason, det, constructs) \
	do { qs_record(queryid, false, (reason), (det), (constructs), heap->planTree->total_cost, cost.rows); return NULL; } while (0)

	if (qs_route == QS_ROUTE_HEAP)
		QS_HEAP(QR_MODE_HEAP, NULL, NIL);
	if (orig->rowMarks != NIL || orig->hasModifyingCTE || orig->utilityStmt != NULL || qs_explain_into)
		QS_HEAP(QR_NOT_PLAIN_SELECT, NULL, NIL);
	if ((state = qs_state_blocks(&state_detail)) >= 0)
		QS_HEAP(state, state_detail, NIL);
	if (cursorOptions & CURSOR_OPT_SCROLL)
		QS_HEAP(QR_CURSOR, NULL, NIL);
	if (qs_is_duckdb_scan(heap->planTree))
		QS_HEAP(QR_NOT_MIRRORED, "the statement is not a heap statement", NIL);

	if (any_other)
		QS_HEAP(QR_NOT_MIRRORED, "also reads a relation that is not registered", NIL);

	ce = qs_cache_get(queryid);
	if (ce && ce->negative && !(qs_route == QS_ROUTE_MIRROR && qs_allow_unsafe))
	{
		snprintf(detail, sizeof(detail), "%s (remembered)", ce->detail);
		QS_HEAP(ce->reason, detail, NIL);
	}

	cheap = qs_route != QS_ROUTE_MIRROR &&
		heap->planTree->total_cost < qs_min_cost && cost.rows < qs_min_rows;
	if (cheap)
	{
		snprintf(detail, sizeof(detail), "cost %.0f under %.0f, %.0f rows from mirrored tables under %.0f",
				 heap->planTree->total_cost, qs_min_cost, cost.rows, qs_min_rows);

		/*
		 * A cheap generic plan would be reused for every later value of its
		 * parameters, including the ones that make the statement expensive.
		 * Unless every mirrored scan is a one-row lookup whatever the value,
		 * price it out so that the plan cache keeps asking with the values.
		 */
		if (boundParams == NULL && qs_prefer_custom && qs_route != QS_ROUTE_SHADOW &&
			cost.max_scan_rows > 1.0 && !(ce && ce->no_params))
		{
			qs_analyse(orig, &w);
			if (w.has_extern_param && w.constructs == NIL && w.n_unmirrored == 0 && w.mirrors != NIL)
			{
				heap->planTree->total_cost += QS_GENERIC_PENALTY;
				QS_HEAP(QR_GENERIC_PLAN, "priced out so that custom plans are kept", NIL);
			}
			if (!w.has_extern_param && (ce = qs_cache_put(queryid)) != NULL)
				ce->no_params = true;
		}
		QS_HEAP(QR_CHEAP, detail, NIL);
	}

	qs_analyse(orig, &w);
	if (w.mirrors == NIL && w.n_unmirrored == 0)
		return NULL;			/* reads no table: nobody's business */
	if (w.n_unmirrored > 0)
	{
		QsConstruct *c = NULL;

		foreach(lc, w.constructs)
		{
			c = (QsConstruct *) lfirst(lc);
			if (strcmp(c->kind, "relation") == 0)
				break;
		}
		snprintf(detail, sizeof(detail), "%s: %s", c ? c->ident : "?", (c && c->detail) ? c->detail : "");
		QS_HEAP(QR_NOT_MIRRORED, detail, w.constructs);
	}
	if (w.constructs != NIL)
	{
		QsConstruct *c = (QsConstruct *) linitial(w.constructs);

		snprintf(detail, sizeof(detail), "%s %s%s%s%s", c->kind, c->ident,
				 c->detail ? " (" : "", c->detail ? c->detail : "", c->detail ? ")" : "");
		if (list_length(w.constructs) > 1)
			snprintf(detail + strlen(detail), sizeof(detail) - strlen(detail), " and %d more",
					 list_length(w.constructs) - 1);
		if (qs_route == QS_ROUTE_MIRROR && qs_allow_unsafe)
			lenient = true;
		else
			QS_HEAP(QR_UNSAFE, detail, w.constructs);	/* not remembered: the walk is cheap and its list is the product */
	}
	if (w.has_extern_param)
	{
		bool		bound = boundParams != NULL && boundParams->paramFetch == NULL;
		int			p = -1;

		while (bound && (p = bms_next_member(w.params, p)) >= 0)
			if (p <= 0 || p > boundParams->numParams ||
				!(boundParams->params[p - 1].pflags & PARAM_FLAG_CONST))
				bound = false;
		if (!bound)
		{
			if (qs_prefer_custom && qs_route != QS_ROUTE_SHADOW && !lenient)
			{
				heap->planTree->total_cost += QS_GENERIC_PENALTY;
				QS_HEAP(QR_GENERIC_PLAN, "priced out so that custom plans are kept", NIL);
			}
			QS_HEAP(QR_GENERIC_PLAN, NULL, NIL);
		}
	}

	/*
	 * Freshness is a fact about the moment of execution, and a plan may be
	 * kept: the routed plan checks it when it starts (qs_begin_scan). Shadow
	 * mode never executes one, so it reports it here.
	 */
	if (qs_route == QS_ROUTE_SHADOW)
	{
		now = qs_current_lsn();
		foreach(lc, w.mirrors)
			if (!qs_fresh((QsMirror *) lfirst(lc), now, detail, sizeof(detail)))
				QS_HEAP(QR_STALE, detail, NIL);
	}

	/* the executor will say so properly; do not even ask the mirror */
	if (!ExecCheckPermissions(heap->rtable, heap->permInfos, false))
		QS_HEAP(QR_NO_PRIVILEGE, NULL, NIL);

	/* and a role that is not a superuser is given the mirror only where DuckDB is confined */
	if (!qs_mirror_allowed(detail, sizeof(detail)))
		QS_HEAP(QR_UNCONFINED, detail, NIL);

	/*
	 * Freshness, now: planning the mirror's statement is what fixes the lake
	 * snapshot it will read (see qs_pin), so the mirror has to have reached
	 * what this statement can see BEFORE pg_duckdb is asked. It is given
	 * quicksilver.freshness_wait to. A mirror nobody reports an LSN for (the
	 * primary has none; a halted table) ends here too, without a failed plan
	 * to remember for a minute. The routed plan does not ask again when it
	 * starts, unless it is kept and run in a later transaction.
	 */
	if (qs_route != QS_ROUTE_SHADOW)
	{
		List	   *heaps = NIL;
		int			stale;

		foreach(lc, w.mirrors)
			heaps = lappend_oid(heaps, ((QsMirror *) lfirst(lc))->heap);
		/* the position this statement can see is taken once, however often it is decided */
		if (qs_decide_now == InvalidXLogRecPtr)
			qs_decide_now = qs_current_lsn();
		stale = qs_gate(heaps, qs_decide_now, false, heap->planTree->total_cost, detail, sizeof(detail));
		if (stale >= 0)
			QS_HEAP(stale, detail, NIL);
		qs_gated_waited_ms = qs_waited_ms;
		qs_pin();
	}

	/*
	 * The session's lakes, the first time it routes. After the gate, so that
	 * a first statement can wait for its mirror like any other; and since
	 * attaching runs a statement in DuckDB, which can deliver an invalidation
	 * of the registry this decision points into, a registry that moved under
	 * it sends the decision round again (it does not attach twice).
	 */
	qs_duck_recycle();
	if (!qs_lakes_attached && qs_lakes != NIL &&
		(qs_attach_retry_at == 0 || GetCurrentTimestamp() >= qs_attach_retry_at))
	{
		(void) qs_attach_lakes(false);
		if (!qs_cache_valid)
		{
			qs_registry_moved = true;
			if (!qs_decide_again)
				return NULL;	/* not a decision yet: the caller comes straight back */
			QS_HEAP(QR_REGISTRY_CHANGED, NULL, NIL);
		}
	}

	/*
	 * Not qs_generation: that moves with every relcache invalidation in the
	 * database (an autovacuum of any table), and a statement would stay on
	 * the heap for no reason. What matters is whether the registry this
	 * decision was made from is still the one in force.
	 */
	generation = qs_load_count;
	mq = copyObject(orig);
	memset(&subst, 0, sizeof(subst));
	subst.params = boundParams;
	qs_subst_query(mq, &subst);
	if (!subst.failed && w.has_extern_param)
		mq = (Query *) qs_param_mutator((Node *) mq, &subst);
	if (subst.failed || generation != qs_load_count || !qs_cache_valid)
	{
		qs_registry_moved = true;	/* the caller decides again, once, from the registry as it is now */
		if (!qs_decide_again)
			return NULL;
		QS_HEAP(QR_REGISTRY_CHANGED, NULL, NIL);
	}
	qs_relids_walker((Node *) mq, &mirror_relids);
	foreach(lc, w.mirrors)
		heap_relids = lappend_oid(heap_relids, ((QsMirror *) lfirst(lc))->heap);

	/*
	 * Ask pg_duckdb for a plan. It prepares the statement in DuckDB here, so
	 * a missing file, a function DuckDB does not have or a type it will not
	 * take all surface now, as an error we can turn into "heap". No
	 * subtransaction: pg_duckdb refuses to work inside one.
	 */
	old = CurrentMemoryContext;
	qs_busy = true;
	qs_elev_begin(&elev);
	PG_TRY();
	{
		if (prev_planner_hook)
			mplan = prev_planner_hook(mq, query_string, cursorOptions, NULL);
		else
			mplan = standard_planner(mq, query_string, cursorOptions, NULL);
	}
	PG_CATCH();
	{
		ErrorData  *ed;

		qs_busy = false;
		qs_elev_end(&elev);
		elev.on = false;
		MemoryContextSwitchTo(old);
		ed = CopyErrorData();
		if (!qs_mirror_error_ok(ed))
			PG_RE_THROW();
		FlushErrorState();
		strlcpy(detail, ed->message, sizeof(detail));
		mplan = NULL;
		qs_lakes_attached = false;	/* whatever went wrong, attach again before the next attempt */
	}
	PG_END_TRY();
	qs_busy = false;
	qs_elev_end(&elev);

	/*
	 * In a transaction block pg_duckdb has opened a DuckDB transaction to
	 * prepare this, and would keep it to the block's end. It is ended here:
	 * the plan may never be executed, and the node prepares the statement
	 * again when it starts, in a transaction of its own that begins after
	 * the freshness check above and so reads at least what that approved.
	 */
	if (qs_in_block())
		(void) qs_duck_end(false);
	if (mplan == NULL)
	{
		/*
		 * Remembered, so that a statement the mirror cannot take does not pay
		 * for the attempt every time; but not when it happened inside a
		 * transaction block, where pg_duckdb refuses for reasons that end
		 * with the transaction.
		 */
		if (!IsTransactionBlock())
			qs_cache_negative(queryid, QR_MIRROR_PLAN_FAILED, detail, qs_negative_ttl);
		QS_HEAP(QR_MIRROR_PLAN_FAILED, detail, NIL);
	}
	if (!qs_is_duckdb_scan(mplan->planTree))
	{
		qs_cache_negative(queryid, QR_MIRROR_PLAN_FAILED, "pg_duckdb did not take the statement", qs_negative_ttl);
		QS_HEAP(QR_MIRROR_PLAN_FAILED, "pg_duckdb did not take the statement", NIL);
	}

	if (qs_route == QS_ROUTE_MIRROR)
		snprintf(detail, sizeof(detail), "quicksilver.route = mirror%s", lenient ? ", unsafe constructs allowed" : "");
	else if (cost.rows >= qs_min_rows)
		snprintf(detail, sizeof(detail), "%.0f rows from mirrored tables, over %.0f", cost.rows, qs_min_rows);
	else
		snprintf(detail, sizeof(detail), "cost %.0f, over %.0f", heap->planTree->total_cost, qs_min_cost);

	if (qs_route == QS_ROUTE_SHADOW)
	{
		char		why[QS_DETAIL_LEN];
		PlannedStmt *probe = copyObject(heap);

		if (qs_combine(probe, mplan, queryid, detail, false, why, sizeof(why), mirror_relids, heap_relids) == NULL)
			QS_HEAP(QR_OUTPUT_MISMATCH, why, NIL);
		QS_HEAP(QR_SHADOW, detail, NIL);
	}
	{
		char		why[QS_DETAIL_LEN];
		double		heap_cost = heap->planTree->total_cost;

		result = qs_combine(heap, mplan, queryid, detail, lenient, why, sizeof(why), mirror_relids, heap_relids);
		if (result == NULL)
		{
			qs_cache_negative(queryid, QR_OUTPUT_MISMATCH, why, -1);
			QS_HEAP(QR_OUTPUT_MISMATCH, why, NIL);
		}
		qs_record(queryid, true, QR_ROUTED, detail, lenient ? w.constructs : NIL, heap_cost, cost.rows);
	}
	qs_gated_plan = (void *) result->planTree;
	qs_gated_lxid = MyProc->vxid.lxid;
	return result;
#undef QS_HEAP
}

/* Does the statement call a function of pg_duckdb's schema (duckdb.raw_query, duckdb.query, ...)? */
static bool
qs_duckfn_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, FuncExpr) && get_func_namespace(((FuncExpr *) node)->funcid) == *(Oid *) context)
		return true;
	if (IsA(node, Query))
		return query_tree_walker((Query *) node, qs_duckfn_walker, context, 0);
	return expression_tree_walker(node, qs_duckfn_walker, context);
}

static PlannedStmt *
qs_planner(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *heap;
	PlannedStmt *routed;
	Query	   *orig = NULL;
	bool		any_other = false;
	bool		installed = false;
	List	   *deps = NIL;

	if (qs_route != QS_ROUTE_OFF && !qs_busy && parse->commandType == CMD_SELECT &&
		IsTransactionState() && !IsInParallelMode() && OidIsValid(MyDatabaseId))
	{
		qs_ensure_loaded();

		if (qs_installed && hash_get_num_entries(qs_mirror_hash) > 0)
		{
			QsRels		rels = {0, 0, NIL};

			/* a statement that reads no mirrored table costs this walk and nothing else */
			qs_rels_walker((Node *) parse, &rels);
			if (rels.mirrored > 0)
			{
				orig = copyObject(parse);	/* the planner scribbles on its input */
				any_other = rels.other > 0;
				deps = rels.mirror_oids;
			}
		}
		installed = qs_installed;
	}

	/*
	 * Somebody else using DuckDB in this transaction: a function of
	 * pg_duckdb's schema here, or below a statement pg_duckdb plans itself.
	 * See qs_duck_foreign.
	 */
	if (qs_route != QS_ROUTE_OFF && !qs_busy && !qs_duck_foreign && IsTransactionState() && qs_in_block())
	{
		Oid			nsp = get_namespace_oid("duckdb", true);

		if (OidIsValid(nsp) && qs_duckfn_walker((Node *) parse, &nsp))
			qs_duck_foreign = true;
	}

	if (prev_planner_hook)
		heap = prev_planner_hook(parse, query_string, cursorOptions, boundParams);
	else
		heap = standard_planner(parse, query_string, cursorOptions, boundParams);
	if (!qs_busy && heap->planTree &&
		(qs_is_duckdb_scan(heap->planTree) || qs_is_duckdb_scan(outerPlan(heap->planTree))))
		qs_duck_foreign = true;

	/*
	 * A plan that is kept (a prepared statement without parameters is planned
	 * once) must be thrown away when the decision behind it could change:
	 * the registry for every SELECT, since registering a table makes it a
	 * candidate; the allowlist and the mirror relations for the candidates.
	 * The plan cache does that for any relation listed here.
	 */
	if (installed && heap->commandType == CMD_SELECT && heap->utilityStmt == NULL)
	{
		heap->relationOids = list_append_unique_oid(heap->relationOids, qs_mirrors_relid);
		if (orig != NULL)
		{
			heap->relationOids = list_append_unique_oid(heap->relationOids, qs_allowlist_relid);
			heap->relationOids = list_concat_unique_oid(heap->relationOids, deps);
		}
	}
	if (orig == NULL)
		return heap;
	qs_registry_moved = false;
	qs_decide_again = false;
	qs_decide_now = InvalidXLogRecPtr;
	routed = qs_decide(orig, heap, query_string, cursorOptions, boundParams, any_other);
	if (routed == NULL && qs_registry_moved)
	{
		/*
		 * An invalidation of the registry (or of a table it names: an
		 * ANALYZE is enough) arrived while the mirror's statement was being
		 * built. Nothing was wrong with the statement: decide once more.
		 */
		qs_registry_moved = false;
		qs_decide_again = true;
		qs_ensure_loaded();
		if (qs_installed)
			routed = qs_decide(orig, heap, query_string, cursorOptions, boundParams, any_other);
		qs_decide_again = false;
	}
	return routed ? routed : heap;
}

/* ---------------------------------------------------------------- nesting */

static void
qs_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
{
	qs_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
		else
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
	}
	PG_FINALLY();
	{
		qs_nesting--;
	}
	PG_END_TRY();
}

static void
qs_ExecutorFinish(QueryDesc *queryDesc)
{
	qs_nesting++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
	}
	PG_FINALLY();
	{
		qs_nesting--;
	}
	PG_END_TRY();
}

static void
qs_ProcessUtility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
				  ProcessUtilityContext context, ParamListInfo params,
				  QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *stmt = pstmt->utilityStmt;

	/*
	 * A statement planned under a utility command is somebody's body (DO,
	 * CALL, CREATE TABLE AS, COPY) and stays on the heap. EXPLAIN, EXECUTE
	 * and DECLARE CURSOR are the three that only carry a client's SELECT.
	 */
	bool		transparent = IsA(stmt, ExplainStmt) || IsA(stmt, ExecuteStmt) ||
		IsA(stmt, DeclareCursorStmt) || IsA(stmt, PrepareStmt) || IsA(stmt, FetchStmt) ||
		IsA(stmt, TransactionStmt) || IsA(stmt, VariableSetStmt);

	if (IsA(stmt, CallStmt) && ((CallStmt *) stmt)->funcexpr &&
		get_func_namespace(((CallStmt *) stmt)->funcexpr->funcid) == get_namespace_oid("duckdb", true))
		qs_duck_foreign = true;
	if (!transparent)
		qs_nesting++;
	PG_TRY();
	{
		if (prev_ProcessUtility)
			prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
	}
	PG_FINALLY();
	{
		if (!transparent)
			qs_nesting--;
	}
	PG_END_TRY();
}

static void
qs_ExplainOneQuery(Query *query, int cursorOptions, IntoClause *into, ExplainState *es,
				   const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv)
{
	bool		save_into = qs_explain_into;

	qs_last.fresh = false;
	qs_explain_into = (into != NULL);
	PG_TRY();
	{
		if (prev_ExplainOneQuery)
			prev_ExplainOneQuery(query, cursorOptions, into, es, queryString, params, queryEnv);
		else
		{
			PlannedStmt *plan;
			instr_time	planstart,
						planduration;
			BufferUsage bufusage_start,
						bufusage;

			if (es->buffers)
				bufusage_start = pgBufferUsage;
			INSTR_TIME_SET_CURRENT(planstart);
			plan = pg_plan_query(query, queryString, cursorOptions, params);
			INSTR_TIME_SET_CURRENT(planduration);
			INSTR_TIME_SUBTRACT(planduration, planstart);
			if (es->buffers)
			{
				memset(&bufusage, 0, sizeof(BufferUsage));
				BufferUsageAccumDiff(&bufusage, &pgBufferUsage, &bufusage_start);
			}
			ExplainOnePlan(plan, into, es, queryString, params, queryEnv, &planduration,
						   es->buffers ? &bufusage : NULL, NULL);
		}
	}
	PG_FINALLY();
	{
		qs_explain_into = save_into;
	}
	PG_END_TRY();

	if (qs_last.fresh && !qs_last.mirror)
	{
		ExplainPropertyText("Quicksilver",
							psprintf("heap (%s)", qs_reason_names[qs_last.reason]), es);
		if ((es->costs || es->verbose) && qs_last.detail[0])
			ExplainPropertyText("Quicksilver Detail", qs_last.detail, es);
	}
}

/* ================================================================ SQL-callable */

Datum
qs_changed(PG_FUNCTION_ARGS)
{
	TriggerData *td = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "quicksilver._changed() is a trigger function");
	/* transactional, and it reaches every backend and every standby */
	CacheInvalidateRelcacheByRelid(RelationGetRelid(td->tg_relation));
	return PointerGetDatum(NULL);
}

/*
 * Remove this database's entries for relations that are no longer registered
 * heap tables. The caller holds the lock exclusively and has the registry
 * loaded; nothing here can throw.
 */
static void
qs_lsn_sweep_own(void)
{
	HASH_SEQ_STATUS seq;
	QsLsnEntry *e;

	hash_seq_init(&seq, qs_lsn_hash);
	while ((e = hash_seq_search(&seq)) != NULL)
	{
		if (e->key.dbid != MyDatabaseId)
			continue;
		if (qs_mirror_hash == NULL || hash_search(qs_mirror_hash, &e->key.relid, HASH_FIND, NULL) == NULL)
			hash_search(qs_lsn_hash, &e->key, HASH_REMOVE, NULL);
	}
}

/* Remove the entries of databases that have been dropped. True if any went. */
static bool
qs_lsn_sweep_databases(void)
{
	HASH_SEQ_STATUS seq;
	QsLsnEntry *e;
	List	   *dbs = NIL;
	List	   *dead = NIL;
	ListCell   *lc;
	bool		any = false;

	LWLockAcquire(qs_shared->lock, LW_SHARED);
	hash_seq_init(&seq, qs_lsn_hash);
	while ((e = hash_seq_search(&seq)) != NULL)
		if (e->key.dbid != MyDatabaseId)
			dbs = list_append_unique_oid(dbs, e->key.dbid);
	LWLockRelease(qs_shared->lock);
	foreach(lc, dbs)			/* a catalog lookup: not under the lock */
		if (get_database_name(lfirst_oid(lc)) == NULL)
			dead = lappend_oid(dead, lfirst_oid(lc));
	if (dead == NIL)
		return false;
	LWLockAcquire(qs_shared->lock, LW_EXCLUSIVE);
	hash_seq_init(&seq, qs_lsn_hash);
	while ((e = hash_seq_search(&seq)) != NULL)
		if (list_member_oid(dead, e->key.dbid))
		{
			hash_search(qs_lsn_hash, &e->key, HASH_REMOVE, NULL);
			any = true;
		}
	LWLockRelease(qs_shared->lock);
	return any;
}

Datum
qs_report_applied_lsn(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	XLogRecPtr	lsn = PG_GETARG_LSN(1);
	QsLsnKey	key;
	QsLsnEntry *e;
	bool		found;
	bool		registered;
	bool		sweep;

	if (!qs_shared)
		ereport(ERROR, (errmsg("pg_quicksilver must be in shared_preload_libraries")));
	memset(&key, 0, sizeof(key));
	key.dbid = MyDatabaseId;
	key.relid = relid;

	/*
	 * Only a registered heap table has an entry, and an entry goes when its
	 * registration does: the table is fixed in size, and tables that were
	 * dropped, re-created or unregistered must not use it up.
	 */
	qs_ensure_loaded();
	registered = qs_installed && qs_mirror_lookup(relid) != NULL;
	sweep = qs_lsn_swept != qs_load_count;
	LWLockAcquire(qs_shared->lock, LW_EXCLUSIVE);
	if (sweep || (registered && hash_get_num_entries(qs_lsn_hash) >= QS_LSN_MAX))
		qs_lsn_sweep_own();
	if (!registered)
	{
		hash_search(qs_lsn_hash, &key, HASH_REMOVE, NULL);
		LWLockRelease(qs_shared->lock);
		qs_lsn_swept = qs_load_count;
		PG_RETURN_VOID();
	}
	e = hash_search(qs_lsn_hash, &key,
					hash_get_num_entries(qs_lsn_hash) < QS_LSN_MAX ? HASH_ENTER : HASH_FIND, &found);
	if (e)
	{
		e->lsn = lsn;			/* the reporter knows; it may go backwards after a rebuild */
		e->at = GetCurrentTimestamp();
	}
	LWLockRelease(qs_shared->lock);
	qs_lsn_swept = qs_load_count;
	if (!e && qs_lsn_sweep_databases())
	{
		/* entries of databases that no longer exist were in the way */
		LWLockAcquire(qs_shared->lock, LW_EXCLUSIVE);
		e = hash_search(qs_lsn_hash, &key,
						hash_get_num_entries(qs_lsn_hash) < QS_LSN_MAX ? HASH_ENTER : HASH_FIND, &found);
		if (e)
		{
			e->lsn = lsn;
			e->at = GetCurrentTimestamp();
		}
		LWLockRelease(qs_shared->lock);
	}
	if (!e)
		ereport(ERROR, (errmsg("too many mirrored relations (%d)", QS_LSN_MAX)));
	/* whoever is waiting for this mirror looks again now */
	ConditionVariableBroadcast(&qs_shared->reported);
	PG_RETURN_VOID();
}

Datum
qs_mirror_status(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS seq;
	QsMirror   *m;
	XLogRecPtr	now;

	InitMaterializedSRF(fcinfo, 0);
	qs_ensure_loaded();
	if (!qs_installed)
		PG_RETURN_VOID();
	now = qs_current_lsn();
	hash_seq_init(&seq, qs_mirror_hash);
	while ((m = hash_seq_search(&seq)) != NULL)
	{
		Datum		values[9];
		bool		nulls[9] = {0};
		XLogRecPtr	applied = qs_applied_lsn(m);
		char		why[QS_DETAIL_LEN] = "";
		bool		fresh = qs_fresh(m, now, why, sizeof(why));
		StringInfoData cols;
		int			i;

		initStringInfo(&cols);
		for (i = 0; i < m->natts; i++)
			if (m->col_ok && !m->col_ok[i] && m->col_why[i] && strcmp(m->col_why[i], "dropped") != 0)
				appendStringInfo(&cols, "%s%s: %s", cols.len ? "; " : "",
								 get_attname(m->heap, i + 1, true), m->col_why[i]);
		values[0] = ObjectIdGetDatum(m->heap);
		values[1] = ObjectIdGetDatum(m->mirror);
		values[2] = BoolGetDatum(m->usable);
		if (m->reason)
			values[3] = CStringGetTextDatum(m->reason);
		else
			nulls[3] = true;
		if (cols.len)
			values[4] = CStringGetTextDatum(cols.data);
		else
			nulls[4] = true;
		if (applied != InvalidXLogRecPtr)
			values[5] = LSNGetDatum(applied);
		else
			nulls[5] = true;
		values[6] = LSNGetDatum(now);
		values[7] = BoolGetDatum(fresh);
		if (why[0])
			values[8] = CStringGetTextDatum(why);
		else
			nulls[8] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	PG_RETURN_VOID();
}

/* quicksilver.attach_lakes(): for a session that reads a mirror relation by name */
Datum
qs_attach_lakes_sql(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("quicksilver.attach_lakes() is for superusers"),
				 errdetail("It attaches the lakes in this session's DuckDB for a role that reads a mirror relation by name.")));
	qs_ensure_loaded();
	qs_duck_foreign = true;
	PG_RETURN_INT32(qs_installed ? qs_attach_lakes(true) : 0);
}

/* quicksilver.unsafe_constructs(sql): the constructs that keep a statement on the heap */
Datum
qs_check(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	char	   *sql = text_to_cstring(PG_GETARG_TEXT_PP(0));
	List	   *raw;
	List	   *rewritten;
	Oid		   *ptypes = NULL;
	int			nparams = 0;
	QsWalk		w;
	ListCell   *lc;
	Datum		values[3];
	bool		nulls[3];

	InitMaterializedSRF(fcinfo, 0);
	qs_ensure_loaded();
	if (!qs_installed)
		ereport(ERROR, (errmsg("pg_quicksilver's tables are missing from this database")));
	memset(&w, 0, sizeof(w));
	raw = pg_parse_query(sql);
	if (list_length(raw) != 1)
		qs_add(&w, "feature", "not_select", "not exactly one statement");
	else
	{
		rewritten = pg_analyze_and_rewrite_varparams(linitial_node(RawStmt, raw), sql, &ptypes, &nparams, NULL);
		if (list_length(rewritten) != 1)
			qs_add(&w, "feature", "not_select", "rewrites to several statements");
		else
			qs_analyse(linitial_node(Query, rewritten), &w);
	}
	foreach(lc, w.constructs)
	{
		QsConstruct *c = (QsConstruct *) lfirst(lc);

		memset(nulls, 0, sizeof(nulls));
		values[0] = CStringGetTextDatum(c->kind);
		values[1] = CStringGetTextDatum(c->ident);
		if (c->detail)
			values[2] = CStringGetTextDatum(c->detail);
		else
			nulls[2] = true;
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	PG_RETURN_VOID();
}

Datum
qs_last_decision(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Datum		values[7];
	bool		nulls[7] = {0};

	InitMaterializedSRF(fcinfo, 0);
	if (!qs_last.valid)
		PG_RETURN_VOID();
	values[0] = CStringGetTextDatum(qs_last.mirror ? "mirror" : "heap");
	values[1] = CStringGetTextDatum(qs_reason_names[qs_last.reason]);
	values[2] = CStringGetTextDatum(qs_last.detail);
	values[3] = DirectFunctionCall1(jsonb_in, CStringGetDatum(qs_last.constructs.data ? qs_last.constructs.data : "[]"));
	values[4] = Int64GetDatum((int64) qs_last.queryid);
	values[5] = Float8GetDatum(qs_last.cost);
	values[6] = Float8GetDatum(qs_last.rows);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	PG_RETURN_VOID();
}

Datum
qs_forget_last(PG_FUNCTION_ARGS)
{
	qs_last.valid = false;
	PG_RETURN_VOID();
}

Datum
qs_decisions(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS seq;
	QsStatEntry *e;

	InitMaterializedSRF(fcinfo, 0);
	if (!qs_shared)
		PG_RETURN_VOID();
	LWLockAcquire(qs_shared->lock, LW_SHARED);
	hash_seq_init(&seq, qs_stat_hash);
	while ((e = hash_seq_search(&seq)) != NULL)
	{
		Datum		values[7];
		bool		nulls[7] = {0};

		if (e->key.dbid != MyDatabaseId)
			continue;
		values[0] = Int64GetDatum((int64) e->key.queryid);
		values[1] = Int64GetDatum((int64) pg_atomic_read_u64(&e->n_mirror));
		values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&e->n_heap));
		values[3] = Int64GetDatum((int64) pg_atomic_read_u64(&e->n_fallback));
		values[4] = CStringGetTextDatum(e->last_mirror ? "mirror" : "heap");
		values[5] = CStringGetTextDatum(qs_reason_names[e->last_reason]);
		values[6] = CStringGetTextDatum(e->last_detail);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	LWLockRelease(qs_shared->lock);
	PG_RETURN_VOID();
}

Datum
qs_reasons(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	int			i;

	InitMaterializedSRF(fcinfo, 0);
	if (!qs_shared)
		PG_RETURN_VOID();
	LWLockAcquire(qs_shared->lock, LW_SHARED);
	for (i = 0; i < QR_NREASONS; i++)
	{
		Datum		values[3];
		bool		nulls[3] = {0};

		values[0] = CStringGetTextDatum(i == QR_ROUTED ? "mirror" : "heap");
		values[1] = CStringGetTextDatum(qs_reason_names[i]);
		values[2] = Int64GetDatum((int64) pg_atomic_read_u64(&qs_shared->reasons[i]));
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	LWLockRelease(qs_shared->lock);
	PG_RETURN_VOID();
}

Datum
qs_reset(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS seq;
	QsStatEntry *e;

	if (!qs_shared)
		PG_RETURN_VOID();
	LWLockAcquire(qs_shared->lock, LW_EXCLUSIVE);
	hash_seq_init(&seq, qs_stat_hash);
	while ((e = hash_seq_search(&seq)) != NULL)
		hash_search(qs_stat_hash, &e->key, HASH_REMOVE, NULL);
	for (int i = 0; i < QR_NREASONS; i++)
		pg_atomic_write_u64(&qs_shared->reasons[i], 0);
	pg_atomic_write_u64(&qs_shared->dropped, 0);
	LWLockRelease(qs_shared->lock);
	PG_RETURN_VOID();
}

Datum
qs_allowlist_problems(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	ListCell   *lc;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	qs_cache_valid = false;		/* report on a fresh load */
	qs_ensure_loaded();
	foreach(lc, qs_load_problems)
	{
		Datum		values[1];
		bool		nulls[1] = {0};

		values[0] = CStringGetTextDatum((char *) lfirst(lc));
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	PG_RETURN_VOID();
}

/* ================================================================ setup */

static bool
qs_check_min_lsn(char **newval, void **extra, GucSource source)
{
	XLogRecPtr *lsn;
	bool		have_error = false;

	lsn = (XLogRecPtr *) guc_malloc(LOG, sizeof(XLogRecPtr));
	if (!lsn)
		return false;
	*lsn = InvalidXLogRecPtr;
	if (*newval && (*newval)[0] != '\0')
	{
		*lsn = pg_lsn_in_internal(*newval, &have_error);
		if (have_error)
		{
			guc_free(lsn);
			GUC_check_errdetail("\"%s\" is not a valid LSN.", *newval);
			return false;
		}
	}
	*extra = lsn;
	return true;
}

static void
qs_assign_min_lsn(const char *newval, void *extra)
{
	qs_min_applied_lsn = extra ? *(XLogRecPtr *) extra : InvalidXLogRecPtr;
}

/* A kept plan was made under the old mode; none of them may outlive it. */
static void
qs_assign_route(int newval, void *extra)
{
	if (newval != qs_route && IsUnderPostmaster && IsTransactionState())
		ResetPlanCache();
}

static Size
qs_shmem_size(void)
{
	Size		size = MAXALIGN(sizeof(QsShared));

	size = add_size(size, hash_estimate_size(qs_stats_max, sizeof(QsStatEntry)));
	size = add_size(size, hash_estimate_size(QS_LSN_MAX, sizeof(QsLsnEntry)));
	return size;
}

static void
qs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(qs_shmem_size());
	RequestNamedLWLockTranche("pg_quicksilver", 1);
}

static void
qs_shmem_startup(void)
{
	bool		found;
	HASHCTL		ctl;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	qs_shared = ShmemInitStruct("pg_quicksilver", sizeof(QsShared), &found);
	if (!found)
	{
		memset(qs_shared, 0, sizeof(QsShared));
		for (int i = 0; i < QR_NREASONS; i++)
			pg_atomic_init_u64(&qs_shared->reasons[i], 0);
		pg_atomic_init_u64(&qs_shared->dropped, 0);
		ConditionVariableInit(&qs_shared->reported);
		qs_shared->lock = &(GetNamedLWLockTranche("pg_quicksilver"))->lock;
	}
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QsStatKey);
	ctl.entrysize = sizeof(QsStatEntry);
	qs_stat_hash = ShmemInitHash("pg_quicksilver decisions", qs_stats_max, qs_stats_max, &ctl,
								 HASH_ELEM | HASH_BLOBS);
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QsLsnKey);
	ctl.entrysize = sizeof(QsLsnEntry);
	qs_lsn_hash = ShmemInitHash("pg_quicksilver applied LSNs", QS_LSN_MAX, QS_LSN_MAX, &ctl,
								HASH_ELEM | HASH_BLOBS);
	LWLockRelease(AddinShmemInitLock);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("pg_quicksilver must be loaded via shared_preload_libraries"),
				 errhint("List it after pg_duckdb.")));

	DefineCustomEnumVariable("quicksilver.route",
							 "Where read queries on mirrored tables are answered.",
							 "off: the extension does nothing. shadow: decide and report, never act. "
							 "auto: route when safe, fresh and expensive. heap: never route. "
							 "mirror: route whenever safe and fresh, whatever the cost.",
							 &qs_route, QS_ROUTE_OFF, qs_route_options, PGC_USERSET, 0, NULL, qs_assign_route, NULL);
	DefineCustomRealVariable("quicksilver.min_cost",
							 "Route when the heap plan's total cost is at least this.", NULL,
							 &qs_min_cost, 10000.0, 0.0, DBL_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomRealVariable("quicksilver.min_rows",
							 "Route when the heap plan reads at least this many rows from mirrored tables.", NULL,
							 &qs_min_rows, 50000.0, 0.0, DBL_MAX, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("quicksilver.max_lag",
							"How far behind this node a mirror may be and still be routed to, unless its registry row says otherwise.",
							"0 (the default) is strict: a statement is routed only if its mirror has reached everything the statement "
							"can see on this node when it starts. Anything larger is an explicit choice: a routed read may then be "
							"that much WAL behind what the heap on this node would have answered.",
							&qs_max_lag_kb, 0, 0, INT_MAX, PGC_USERSET, GUC_UNIT_KB, NULL, NULL, NULL);
	DefineCustomIntVariable("quicksilver.freshness_wait",
							"The least a statement waits for its mirror to reach the position the statement can see, whatever it costs on the heap.",
							"0 (the default) leaves the wait to quicksilver.freshness_wait_fraction and quicksilver.freshness_wait_max.",
							&qs_fresh_wait_ms, 0, 0, 60000, PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomRealVariable("quicksilver.freshness_wait_fraction",
							 "A statement waits for its mirror for at most this fraction of the time its heap plan is estimated to take.",
							 "The estimate is the plan's total cost times quicksilver.cost_unit_ms. 0 turns the proportional wait off.",
							 &qs_wait_fraction, 0.5, 0.0, 1.0, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("quicksilver.freshness_wait_max",
							"The most a statement waits for its mirror under quicksilver.freshness_wait_fraction.", NULL,
							&qs_wait_max_ms, 1000, 0, 60000, PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);
	DefineCustomRealVariable("quicksilver.cost_unit_ms",
							 "Milliseconds of heap execution per unit of planner cost, for the freshness wait.",
							 "Set it low: an under-estimate of the heap makes a statement wait less. "
							 "Measure it with EXPLAIN ANALYZE of the statements that matter (execution time / total cost) and take the smallest.",
							 &qs_cost_unit_ms, 0.002, 0.0, 1000.0, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomStringVariable("quicksilver.min_applied_lsn",
							   "Route only to a mirror that has applied at least this LSN (read your writes).", NULL,
							   &qs_min_applied_lsn_str, "", PGC_USERSET, 0,
							   qs_check_min_lsn, qs_assign_min_lsn, NULL);
	DefineCustomBoolVariable("quicksilver.allow_unsafe",
							 "With quicksilver.route = mirror, route statements the allowlist does not cover.",
							 "For generating and testing the allowlist only: answers may differ from the heap's.",
							 &qs_allow_unsafe, false, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("quicksilver.route_in_transaction",
							 "Route inside a transaction block where that is safe.",
							 "READ COMMITTED, no savepoint open, nothing written yet. The mirror's answer is taken whole when the statement starts "
							 "and DuckDB's transaction is ended with it, so a later SAVEPOINT, write or EXPLAIN finds pg_duckdb idle.",
							 &qs_route_in_xact, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("quicksilver.prefer_custom_plans",
							 "Keep the plan cache planning routable prepared statements with their parameter values.", NULL,
							 &qs_prefer_custom, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("quicksilver.fallback",
							 "Answer from the heap when the mirror fails before its first row.", NULL,
							 &qs_fallback, true, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("quicksilver.log_decisions",
							 "Log every routing decision with the constructs that kept a statement on the heap.", NULL,
							 &qs_log_decisions, false, PGC_SUSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("quicksilver.tolerate_lost_errors",
							 "Route integer + - *, which can overflow.",
							 "Where PostgreSQL would raise \"integer out of range\", the mirror may return an answer instead: "
							 "its optimizer can rewrite the expression so that the overflow never happens.",
							 &qs_tolerate_lost_errors, false, PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomIntVariable("quicksilver.negative_cache_ttl",
							"Seconds a statement the mirror failed on stays on the heap without asking again.", NULL,
							&qs_negative_ttl, 60, 0, 86400, PGC_USERSET, GUC_UNIT_S, NULL, NULL, NULL);
	DefineCustomIntVariable("quicksilver.stats_max",
							"Statements tracked by quicksilver.decisions.", NULL,
							&qs_stats_max, 2000, 100, 1000000, PGC_POSTMASTER, 0, NULL, NULL, NULL);
	MarkGUCPrefixReserved("quicksilver");

	EnableQueryId();
	RegisterXactCallback(qs_xact_cb, NULL);

	memset(&qs_scan_methods, 0, sizeof(qs_scan_methods));
	qs_scan_methods.CustomName = QS_SCAN_NAME;
	qs_scan_methods.CreateCustomScanState = qs_create_scan_state;
	RegisterCustomScanMethods(&qs_scan_methods);
	memset(&qs_exec_methods, 0, sizeof(qs_exec_methods));
	qs_exec_methods.CustomName = QS_SCAN_NAME;
	qs_exec_methods.BeginCustomScan = qs_begin_scan;
	qs_exec_methods.ExecCustomScan = qs_exec_scan;
	qs_exec_methods.EndCustomScan = qs_end_scan;
	qs_exec_methods.ReScanCustomScan = qs_rescan;
	qs_exec_methods.ExplainCustomScan = qs_explain_scan;

	CacheRegisterRelcacheCallback(qs_relcache_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(PROCOID, qs_syscache_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(OPEROID, qs_syscache_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(TYPEOID, qs_syscache_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(COLLOID, qs_syscache_cb, (Datum) 0);
	CacheRegisterSyscacheCallback(NAMESPACEOID, qs_syscache_cb, (Datum) 0);

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = qs_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = qs_shmem_startup;
	prev_planner_hook = planner_hook;
	planner_hook = qs_planner;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = qs_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = qs_ExecutorFinish;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = qs_ProcessUtility;
	prev_ExplainOneQuery = ExplainOneQuery_hook;
	ExplainOneQuery_hook = qs_ExplainOneQuery;
}
