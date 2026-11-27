-- Bare columns: what the mirror hands back for each source type with no
-- expression in the way. This is the type mapping measured on its own.

-- @id col-001
SELECT id, si FROM t_num ORDER BY id;
-- @id col-002
SELECT id, i FROM t_num ORDER BY id;
-- @id col-003
SELECT id, bi FROM t_num ORDER BY id;
-- @id col-004
SELECT id, n FROM t_num ORDER BY id;
-- @id col-005
SELECT id, n0 FROM t_num ORDER BY id;
-- @id col-006
SELECT id, nu FROM t_num ORDER BY id;
-- @id col-007
SELECT id, r FROM t_num ORDER BY id;
-- @id col-008
SELECT id, d FROM t_num ORDER BY id;
-- @id col-009
SELECT id, b FROM t_num ORDER BY id;
-- @id col-010
SELECT id, t FROM t_str ORDER BY id;
-- @id col-011
SELECT id, vc FROM t_str ORDER BY id;
-- @id col-012
SELECT id, ch FROM t_str ORDER BY id;
-- @id col-013
SELECT id, u FROM t_str ORDER BY id;
-- @id col-014
SELECT id, e FROM t_str ORDER BY id;
-- @id col-015
SELECT id, ip FROM t_str ORDER BY id;
-- @id col-016
SELECT id, raw FROM t_str ORDER BY id;
-- @id col-017
SELECT id, d FROM t_time ORDER BY id;
-- @id col-018
SELECT id, tm FROM t_time ORDER BY id;
-- @id col-019
SELECT id, ts FROM t_time ORDER BY id;
-- @id col-020
SELECT id, tz FROM t_time ORDER BY id;
-- @id col-021
SELECT id, iv FROM t_time ORDER BY id;
-- @id col-022
SELECT id, ttz FROM t_time ORDER BY id;
-- @id col-023
SELECT id, j FROM t_doc ORDER BY id;
-- @id col-024
SELECT id, jb FROM t_doc ORDER BY id;
-- @id col-025
SELECT id, ia FROM t_doc ORDER BY id;
-- @id col-026
SELECT id, ta FROM t_doc ORDER BY id;
-- @id col-027
SELECT id, na FROM t_doc ORDER BY id;
-- @id col-028
SELECT * FROM customers ORDER BY id;
-- @id col-029
SELECT * FROM orders ORDER BY id;
-- @id col-030
SELECT * FROM t_num WHERE id = 1;
-- @id col-031
SELECT t_str.* FROM t_str WHERE id BETWEEN 9001 AND 9007 ORDER BY id;
-- @id col-032
SELECT count(*) AS n, count(si) AS si, count(nu) AS nu, count(r) AS r, count(b) AS b FROM t_num;
