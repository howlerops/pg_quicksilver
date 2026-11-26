-- LIMIT / OFFSET with ORDER BY. The sort key decides WHICH rows come back, so an
-- ordering difference here is a wrong-rows difference, not a presentation one.

-- @id lim-001
SELECT id FROM t_num ORDER BY id LIMIT 10;
-- @id lim-002
SELECT id FROM t_num ORDER BY id DESC LIMIT 10 OFFSET 5;
-- @id lim-003
SELECT id, i FROM t_num ORDER BY i, id LIMIT 10;
-- @id lim-004
SELECT id, i FROM t_num ORDER BY i DESC, id LIMIT 10;
-- @id lim-005
SELECT id, i FROM t_num ORDER BY i DESC NULLS LAST, id LIMIT 10;
-- @id lim-006
SELECT id, n FROM t_num ORDER BY n NULLS FIRST, id LIMIT 10;
-- @id lim-007
SELECT id, d FROM t_num ORDER BY d DESC, id LIMIT 8;
-- @id lim-008
SELECT id, r FROM t_num ORDER BY r, id LIMIT 8;
-- @id lim-009
SELECT id, t FROM t_str ORDER BY t, id LIMIT 15;
-- @id lim-010
SELECT id, t FROM t_str ORDER BY t DESC, id LIMIT 15;
-- @id lim-011
SELECT id, g FROM t_str ORDER BY g, id LIMIT 15 OFFSET 100;
-- @id lim-012
SELECT id, g FROM t_str ORDER BY lower(g), id LIMIT 15 OFFSET 100;
-- @id lim-013
SELECT id, vc FROM t_str ORDER BY vc NULLS FIRST, id LIMIT 15 OFFSET 215;
-- @id lim-014
SELECT id, ch FROM t_str ORDER BY ch, id LIMIT 15 OFFSET 160;
-- @id lim-015
SELECT id, e FROM t_str ORDER BY e DESC, id LIMIT 12;
-- @id lim-016
SELECT id, e FROM t_str ORDER BY e, id LIMIT 12 OFFSET 495;
-- @id lim-017
SELECT id, u FROM t_str ORDER BY u DESC, id LIMIT 12;
-- @id lim-018
SELECT id, d FROM t_time ORDER BY d DESC, id LIMIT 10;
-- @id lim-019
SELECT id, ts FROM t_time ORDER BY ts, id LIMIT 10;
-- @id lim-020
SELECT id, tz FROM t_time ORDER BY tz DESC NULLS LAST, id LIMIT 10;
-- @id lim-021
SELECT id, iv FROM t_time ORDER BY iv DESC, id LIMIT 10;
-- @id lim-022
SELECT id, tm FROM t_time ORDER BY tm, id LIMIT 10 OFFSET 1990;
-- @id lim-023
SELECT id, amount FROM orders ORDER BY amount DESC, id LIMIT 10;
-- @id lim-024
SELECT id, placed FROM orders ORDER BY placed DESC, id LIMIT 10;
-- @id lim-025
SELECT id, status, amount FROM orders ORDER BY status, amount DESC, id LIMIT 20 OFFSET 590;
-- @id lim-026
SELECT name FROM customers ORDER BY name LIMIT 20;
-- @id lim-027
SELECT name FROM customers ORDER BY name DESC LIMIT 20 OFFSET 20;
-- @id lim-028
SELECT region, name FROM customers ORDER BY region, name LIMIT 20 OFFSET 50;
-- @id lim-029
SELECT id FROM t_num ORDER BY id LIMIT 0;
-- @id lim-030
SELECT id FROM t_num ORDER BY id OFFSET 2010;
-- @id lim-031
SELECT id FROM t_num ORDER BY id FETCH FIRST 5 ROWS ONLY;
-- @id lim-032
SELECT id, g FROM t_num ORDER BY g, id FETCH FIRST 3 ROWS WITH TIES;
-- @id lim-033
SELECT id FROM t_num ORDER BY id LIMIT ALL OFFSET 2012;
-- @id lim-034
SELECT id, ip FROM t_str ORDER BY ip, id LIMIT 10;
-- @id lim-035
SELECT id, jb ->> 'name' AS name FROM t_doc ORDER BY jb ->> 'name', id LIMIT 10 OFFSET 30;
-- @id lim-036
SELECT id, (jb ->> 'price')::numeric AS price FROM t_doc ORDER BY (jb ->> 'price')::numeric DESC NULLS LAST, id LIMIT 10;
