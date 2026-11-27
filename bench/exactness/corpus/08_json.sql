-- JSON operators and functions, over json and jsonb columns (both VARCHAR in
-- the mirror) and arrays (also VARCHAR).

-- @id json-001
SELECT id, jb -> 'name' AS name_json, jb ->> 'name' AS name_text FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-002
SELECT id, j -> 'name' AS name_json, j ->> 'name' AS name_text FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-003
SELECT id, jb -> 'n' -> 'x' AS x_json, jb -> 'n' ->> 'x' AS x_text, jb -> 'n' ->> 'y' AS y_text FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-004
SELECT id, jb -> 'tags' -> 0 AS first_tag, jb -> 'tags' ->> 1 AS second_tag, jb -> 'tags' -> 5 AS missing FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-005
SELECT id, jb #> '{n,x}' AS x_json, jb #>> '{tags,0}' AS tag0 FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-006
SELECT id, (jb ->> 'id')::int AS jid, (jb ->> 'price')::numeric AS price, (jb ->> 'ok')::boolean AS ok FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-007
SELECT id, (jb -> 'price')::numeric AS price, (jb -> 'id')::int AS jid, (jb -> 'ok')::boolean AS ok FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-008
SELECT id, (jb ->> 'big')::bigint AS big FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-009
SELECT count(*) AS n FROM t_doc WHERE jb ->> 'name' = 'n7';
-- @id json-010
SELECT count(*) AS n FROM t_doc WHERE (jb ->> 'price')::numeric > 50;
-- @id json-011
SELECT count(*) AS n FROM t_doc WHERE jb @> '{"ok": true}';
-- @id json-012
SELECT count(*) AS n FROM t_doc WHERE jb @> '{"n": {"x": 3}}';
-- @id json-013
SELECT count(*) AS n FROM t_doc WHERE jb ? 'zz';
-- @id json-014
SELECT count(*) AS n FROM t_doc WHERE jb -> 'tags' ? 't3';
-- @id json-015
SELECT count(*) AS n FROM t_doc WHERE jb ?| ARRAY['nope', 'big'];
-- @id json-016
SELECT count(*) AS n FROM t_doc WHERE jb -> 'n' ->> 'y' IS NULL AND jb IS NOT NULL;
-- @id json-017
SELECT jb ->> 'name' AS name, count(*) AS n, sum((jb ->> 'price')::numeric) AS total FROM t_doc WHERE jb IS NOT NULL GROUP BY 1 ORDER BY 1;
-- @id json-018
SELECT id, jb FROM t_doc WHERE id <= 10 OR id >= 9000 ORDER BY id;
-- @id json-019
SELECT id, j FROM t_doc WHERE id <= 10 OR id >= 9000 ORDER BY id;
-- @id json-020
SELECT id, jsonb_typeof(jb) AS t, json_typeof(j) AS jt FROM t_doc WHERE id >= 9000 ORDER BY id;
-- @id json-021
SELECT id, jsonb_typeof(jb -> 'price') AS price_t, jsonb_typeof(jb -> 'tags') AS tags_t, jsonb_typeof(jb -> 'n' -> 'y') AS y_t FROM t_doc WHERE id <= 10 ORDER BY id;
-- @id json-022
SELECT id, jsonb_array_length(jb -> 'tags') AS n_tags FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-023
SELECT id, tag FROM t_doc, jsonb_array_elements_text(jb -> 'tags') AS tag WHERE id <= 10 ORDER BY id, tag;
-- @id json-024
SELECT id, key FROM t_doc, jsonb_object_keys(jb) AS key WHERE id <= 3 ORDER BY id, key;
-- @id json-025
SELECT id, key, value FROM t_doc, jsonb_each_text(jb) AS kv(key, value) WHERE id = 1 ORDER BY key;
-- @id json-026
SELECT id, jb - 'tags' AS without_tags, jb || '{"extra": 1}' AS merged FROM t_doc WHERE id <= 5 ORDER BY id;
-- @id json-027
SELECT id, jsonb_set(jb, '{n,x}', '99') AS patched FROM t_doc WHERE id <= 5 ORDER BY id;
-- @id json-028
SELECT id, jsonb_pretty(jb -> 'n') AS pretty FROM t_doc WHERE id <= 3 ORDER BY id;
-- @id json-029
SELECT id, jb -> 'a' AS dup_key_winner, j -> 'a' AS json_dup_key, j ->> 'a' AS json_dup_text FROM t_doc WHERE id = 9001;
-- @id json-030
SELECT id, jb -> 'm' AS big_number, jb ->> 'n' AS one_point_zero, jb -> 'e' AS empty_obj, jb -> 'l' AS empty_arr FROM t_doc WHERE id = 9007;
-- @id json-031
SELECT id, jb #>> '{k,deep,er,1,z}' AS deep, jb ->> 'u' AS unicode_escape, jb ->> 's' AS quote FROM t_doc WHERE id = 9006;
-- @id json-032
SELECT id, jb -> 0 AS el0, jb ->> 1 AS el1, jb -> -1 AS last FROM t_doc WHERE id = 9002;
-- @id json-033
SELECT to_json(1.50::numeric) AS a, to_jsonb('x'::text) AS b, to_json(ARRAY[1, 2]) AS c, to_jsonb(true) AS d, to_json(NULL::int) AS e;
-- @id json-034
SELECT json_build_object('a', 1, 'b', 'x', 'c', NULL) AS a, jsonb_build_object('b', 1, 'a', 2.50) AS b, json_build_array(1, 'a', NULL, true) AS c;
-- @id json-035
SELECT row_to_json(s) AS j FROM (SELECT id, g, b FROM t_num WHERE id <= 3) s ORDER BY id;
-- @id json-036
SELECT id, to_jsonb(n) AS n, to_jsonb(d) AS d, to_jsonb(b) AS b FROM t_num WHERE id <= 5 ORDER BY id;
-- @id json-037
SELECT '{"a": 1, "b": [1, 2, {"c": null}]}'::jsonb -> 'b' -> 2 AS a, '{"a": 1}'::jsonb ->> 'zzz' AS missing, '[1, 2, 3]'::jsonb @> '[2]' AS contains, '{"a": {"b": 1}}'::jsonb #>> '{a,b}' AS path;
-- @id json-038
SELECT '{"a":1}'::jsonb = '{"a": 1.0}'::jsonb AS num_eq, '{"a":1,"b":2}'::jsonb = '{"b":2,"a":1}'::jsonb AS key_order_eq, '"a"'::jsonb < '"b"'::jsonb AS lt;
-- @id json-039
SELECT id, jb ->> 'zz' AS zz, upper(jb ->> 'zz') AS up, length(jb ->> 'zz') AS len FROM t_doc WHERE id <= 10 ORDER BY id;
-- @id json-040
SELECT id, jsonb_path_query_first(jb, '$.tags[0]') AS tag0, jb @? '$.n.x ? (@ > 5)' AS x_gt_5 FROM t_doc WHERE id <= 20 ORDER BY id;
-- @id json-041
SELECT id, ia[1] AS first, ia[2] AS second, array_length(ia, 1) AS len, cardinality(ia) AS card FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-042
SELECT id, ta[1] AS first, ta[3] AS third, array_length(ta, 1) AS len FROM t_doc WHERE id <= 30 ORDER BY id;
-- @id json-043
SELECT count(*) AS n FROM t_doc WHERE 3 = ANY (ia);
-- @id json-044
SELECT count(*) AS n FROM t_doc WHERE ia @> ARRAY[1, 2];
-- @id json-045
SELECT count(*) AS n FROM t_doc WHERE ia && ARRAY[4];
-- @id json-046
SELECT count(*) AS n FROM t_doc WHERE 'x y' = ANY (ta);
-- @id json-047
SELECT id, el FROM t_doc, unnest(ia) AS el WHERE id <= 8 ORDER BY id, el NULLS LAST;
-- @id json-048
SELECT id, ia || 99 AS appended, array_append(ia, 0) AS app2, array_cat(ia, ARRAY[7, 8]) AS cat FROM t_doc WHERE id <= 10 ORDER BY id;
-- @id json-049
SELECT id, array_to_string(ta, '+', '?') AS joined, array_position(ia, 2) AS pos FROM t_doc WHERE id <= 20 ORDER BY id;
-- @id json-050
SELECT id, ia, ta, na FROM t_doc WHERE id <= 10 OR id >= 9000 ORDER BY id;
-- @id json-051
SELECT id, na[1] AS first, na[1] + na[2] AS total FROM t_doc WHERE id <= 20 ORDER BY id;
-- @id json-052
SELECT id, ia[2] AS cell, array_ndims(ia) AS dims, array_dims(ia) AS shape, array_lower(ia, 1) AS lo FROM t_doc WHERE id = 9002;
-- @id json-053
SELECT ARRAY[1, 2, 3] AS a, ARRAY['a', 'b'] AS b, ARRAY[1, 2] || ARRAY[3] AS c, (ARRAY[10, 20, 30])[2] AS second, (ARRAY[10, 20, 30])[2:3] AS slice, ARRAY[1, 2] = ARRAY[1, 2] AS eq;
-- @id json-054
SELECT id, (SELECT sum(x) FROM unnest(ia) AS x) AS total FROM t_doc WHERE id <= 20 ORDER BY id;
