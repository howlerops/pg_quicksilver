-- String functions: LIKE/ILIKE/SIMILAR/regex, substring, position, trim, lpad,
-- upper/lower on non-ASCII, concat with NULL, length vs octet_length.

-- @id str-001
SELECT count(*) AS n FROM t_str WHERE t LIKE 'a%';
-- @id str-002
SELECT count(*) AS n FROM t_str WHERE t LIKE 'A%';
-- @id str-003
SELECT count(*) AS n FROM t_str WHERE t ILIKE 'a%';
-- @id str-004
SELECT count(*) AS n FROM t_str WHERE t LIKE '%é%';
-- @id str-005
SELECT count(*) AS n FROM t_str WHERE t ILIKE '%É%';
-- @id str-006
SELECT count(*) AS n FROM t_str WHERE t ILIKE 'ü%';
-- @id str-007
SELECT count(*) AS n FROM t_str WHERE t LIKE 'a_b%';
-- @id str-008
SELECT count(*) AS n FROM t_str WHERE t LIKE 'a\_b%';
-- @id str-009
SELECT count(*) AS n FROM t_str WHERE t LIKE 'a\%b%';
-- @id str-010
SELECT count(*) AS n FROM t_str WHERE t LIKE 'a!%b%' ESCAPE '!';
-- @id str-011
SELECT count(*) AS n FROM t_str WHERE t NOT LIKE '%-%';
-- @id str-012
SELECT count(*) AS n FROM t_str WHERE g LIKE '';
-- @id str-013
SELECT count(*) AS n FROM t_str WHERE g LIKE '%';
-- @id str-014
SELECT count(*) AS n FROM t_str WHERE g LIKE 'back\\slash';
-- @id str-015
SELECT count(*) AS n FROM t_str WHERE g LIKE '[bracket]';
-- @id str-016
SELECT count(*) AS n FROM t_str WHERE g ILIKE 'STRASSE';
-- @id str-017
SELECT count(*) AS n FROM t_str WHERE g ILIKE 'İstanbul';
-- @id str-018
SELECT count(*) AS n FROM t_str WHERE t ~ '^[a-c]';
-- @id str-019
SELECT count(*) AS n FROM t_str WHERE t ~* '^[a-c]';
-- @id str-020
SELECT count(*) AS n FROM t_str WHERE t !~ '[0-9]';
-- @id str-021
SELECT count(*) AS n FROM t_str WHERE t ~ 'an';
-- @id str-022
SELECT count(*) AS n FROM t_str WHERE t ~ '^\w+-\d+$';
-- @id str-023
SELECT count(*) AS n FROM t_str WHERE t ~ '^[[:alpha:]]+$';
-- @id str-024
SELECT count(*) AS n FROM t_str WHERE t ~ '\mapple\M';
-- @id str-025
SELECT count(*) AS n FROM t_str WHERE t SIMILAR TO '(apple|banana)%';
-- @id str-026
SELECT count(*) AS n FROM t_str WHERE t SIMILAR TO '[a-c]%-[0-9]+';
-- @id str-027
SELECT id, g, upper(g) AS up, lower(g) AS lo FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-028
SELECT upper('straße') AS a, lower('İSTANBUL') AS b, upper('éclair') AS c, lower('ÀÉÎÕÜ') AS d, upper('ǆ') AS e, initcap('hello wORLD foo-bar') AS f;
-- @id str-029
SELECT id, initcap(g) AS ic FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-030
SELECT id, length(g) AS len, char_length(g) AS clen, octet_length(g) AS olen, bit_length(g) AS blen FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-031
SELECT id, length(ch) AS len, octet_length(ch) AS olen, char_length(ch) AS clen FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-032
SELECT id, length(vc) AS len, length(trim(vc)) AS tlen FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-033
SELECT id, length(t) AS len FROM t_str WHERE id >= 9000 ORDER BY id;
-- @id str-034
SELECT id, substring(t FROM 2 FOR 3) AS s1, substring(t, 2) AS s2, substr(t, 1, 1) AS s3, left(t, 3) AS l, right(t, 2) AS r FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-035
SELECT substring('hello' FROM 0 FOR 3) AS a, substring('hello' FROM -1 FOR 4) AS b, left('hello', -2) AS c, right('hello', -2) AS d, substr('hello', 10) AS e;
-- @id str-036
SELECT id, substring(t FROM '[0-9]+') AS digits, substring(t FROM '^(.*)-') AS stem FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-037
SELECT id, position('-' IN t) AS p, strpos(t, 'a') AS sp FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-038
SELECT position('é' IN 'café au lait') AS a, strpos('日本語テキスト', '語') AS b, position('' IN 'abc') AS c, position('z' IN 'abc') AS d;
-- @id str-039
SELECT id, trim(vc) AS tr, ltrim(g) AS lt, rtrim(g) AS rt, btrim(g, 'ae') AS bt FROM t_str WHERE id <= 60 ORDER BY id;
-- @id str-040
SELECT trim(both 'x' FROM 'xxhixx') AS a, trim(leading '0' FROM '00042') AS b, trim(trailing FROM 'hi   ') AS c, rtrim('abc..', '.') AS d;
-- @id str-041
SELECT id, lpad(id::text, 6, '0') AS code, rpad(g, 10, '.') AS padded FROM t_str WHERE id <= 50 ORDER BY id;
-- @id str-042
SELECT lpad('abc', 2) AS a, lpad('é', 3, 'ñ') AS b, rpad('abc', 6, 'xy') AS c, lpad('abc', 5) AS d, lpad('abc', 0) AS e;
-- @id str-043
SELECT id, t || '!' AS bang, g || NULL AS null_concat, concat(g, NULL, '!') AS fn_concat FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-044
SELECT id, concat(t, '/', vc) AS c, concat_ws('/', t, vc, ch) AS cws FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-045
SELECT 'a' || 1 AS a, 1 || 'a' AS b, 'x' || 1.50 AS c, 'x' || true AS d, 'n=' || NULL::int AS e;
-- @id str-046
SELECT concat('a', 1, 2.50, true, NULL) AS a, concat_ws(',', 1, NULL, 'b', NULL) AS b, concat(NULL, NULL) AS c;
-- @id str-047
SELECT id, replace(t, '-', '_') AS rep, translate(g, 'aeiou', '12345') AS tr FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-048
SELECT id, regexp_replace(t, '[0-9]+', '#') AS first_only, regexp_replace(t, '[aeiou]', '*', 'g') AS all_vowels FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-049
SELECT regexp_replace('Hello World', '(\w+) (\w+)', '\2 \1') AS swapped, regexp_replace('aaa', 'a', 'b') AS first, regexp_replace('AbC', '[a-z]', 'x', 'gi') AS ci;
-- @id str-050
SELECT id, split_part(t, '-', 1) AS stem, split_part(t, '-', 2) AS suffix FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-051
SELECT split_part('a,b,c', ',', 2) AS a, split_part('a,b,c', ',', 5) AS b, split_part('a,b,c', ',', -1) AS c, string_to_array('a,b,,c', ',') AS d;
-- @id str-052
SELECT regexp_match('foo123bar', '([a-z]+)(\d+)') AS m, regexp_split_to_array('a1b22c', '\d+') AS parts;
-- @id str-053
SELECT id, md5(g) AS h FROM t_str WHERE id <= 20 ORDER BY id;
-- @id str-054
SELECT id, reverse(g) AS rev, repeat(left(g, 1), 3) AS rep FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-055
SELECT id, ascii(g) AS code, chr(65 + id % 26) AS letter FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-056
SELECT ascii('é') AS a, ascii('日') AS b, chr(233) AS c, chr(26085) AS d, ascii('') AS e;
-- @id str-057
SELECT id, starts_with(t, 'a') AS sw, t LIKE 'a%' AS lk FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-058
SELECT format('%s has %s items (%I)', 'cart', 3, 'my col') AS a, format('%5s|%-5s|', 'ab', 'cd') AS b, quote_literal('O''Brien') AS c, quote_ident('my col') AS d;
-- @id str-059
SELECT to_hex(255) AS a, encode('hello'::bytea, 'base64') AS b, encode('hello'::bytea, 'hex') AS c, convert_from(decode('68656c6c6f', 'hex'), 'UTF8') AS d;
-- @id str-060
SELECT id, encode(raw, 'hex') AS hex, length(raw) AS len FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-061
SELECT overlay('Txxxxas' PLACING 'hom' FROM 2 FOR 4) AS a, btrim('  x  ') AS b, 'abc' ~~ 'a%' AS c, 'abc' !~~ 'b%' AS d;
-- @id str-062
SELECT id, ch || '|' AS padded, vc || '|' AS vpadded FROM t_str WHERE id <= 30 OR id >= 9000 ORDER BY id;
-- @id str-063
SELECT id, ch::text || '|' AS as_text, length(ch::text) AS len FROM t_str WHERE id <= 30 ORDER BY id;
-- @id str-064
SELECT id, upper(ch) AS up, ch LIKE 'a%' AS lk, ch LIKE '%e' AS ends_e FROM t_str WHERE id <= 60 ORDER BY id;
-- @id str-065
SELECT id, lower(email) AS e, split_part(email, '@', 2) AS domain FROM customers WHERE id <= 30 ORDER BY id;
-- @id str-066
SELECT lower(split_part(email, '@', 2)) AS domain, count(*) AS n FROM customers GROUP BY 1 ORDER BY n DESC, domain;
-- @id str-067
SELECT id, name FROM customers WHERE name ILIKE 'é%' OR name ILIKE 'z%' ORDER BY id LIMIT 30;
-- @id str-068
SELECT id, unaccent_free FROM (SELECT id, translate(name, 'ÉéÖöČč', 'EeOoCc') AS unaccent_free FROM customers WHERE id <= 20) s ORDER BY id;
-- @id str-069
SELECT 'abc' < 'abd' AS a, 'abc' LIKE 'ABC' AS b, 'abc' ILIKE 'ABC' AS c, 'ß' ILIKE 'SS' AS d, 'é' = 'e' || chr(769) AS nfc_vs_nfd;
-- @id str-070
SELECT id, t = normalize(t) AS is_nfc, length(t) AS len FROM t_str WHERE id IN (9, 9005) ORDER BY id;
-- @id str-071
SELECT string_to_array('a b  c', ' ') AS a, array_to_string(ARRAY['a', NULL, 'c'], '-') AS b, array_to_string(ARRAY['a', NULL, 'c'], '-', '?') AS c;
-- @id str-072
SELECT id, t FROM t_str WHERE t ~ '^\s|\s$' ORDER BY id LIMIT 20;
-- @id str-073
SELECT count(*) AS n FROM t_str WHERE t IS NOT NULL AND t = '';
-- @id str-074
SELECT count(*) AS empties, count(*) FILTER (WHERE g IS NULL) AS nulls FROM t_str WHERE coalesce(g, '') = '';
-- @id str-075
SELECT id, left(g, 1) AS initial, upper(left(g, 1)) || lower(substring(g FROM 2)) AS titled FROM t_str WHERE id <= 50 ORDER BY id;
