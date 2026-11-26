-- Deterministic bulk data: arithmetic on generate_series, no random(), no now().
-- Loaded BEFORE the sidecar starts, so it reaches the mirror by snapshot.
SET TimeZone = 'UTC';

INSERT INTO t_num
SELECT g,
  CASE WHEN g % 17 = 0 THEN NULL ELSE ((g * 37) % 2001 - 1000)::smallint END,
  CASE WHEN g % 19 = 0 THEN NULL ELSE (g * 7919) % 200001 - 100000 END,
  CASE WHEN g % 23 = 0 THEN NULL ELSE (g::bigint * 104729 * 1000003) % 9000000000000 - 4500000000000 END,
  CASE WHEN g % 13 = 0 THEN NULL ELSE (((g * 7919) % 1000001 - 500000) / 100.0)::numeric(12,2) END,
  CASE WHEN g % 37 = 0 THEN NULL ELSE ((g * 31) % 100000 - 50000)::numeric(10,0) END,
  CASE WHEN g % 11 = 0 THEN NULL
       ELSE round(((g::bigint * 611953) % 10000000)::numeric / (10 ^ (g % 10))::numeric - 500, g % 10) END,
  CASE WHEN g % 29 = 0 THEN NULL ELSE (((g * 193) % 20001 - 10000) / 7.0)::real END,
  CASE WHEN g % 31 = 0 THEN NULL ELSE ((g * 7919) % 1000003) / 97.0 - 5000 END,
  CASE WHEN g % 7 = 0 THEN NULL ELSE g % 3 = 0 END,
  g % 10
FROM generate_series(1, 2000) g;

-- Mixed case, accents, non-ASCII, digits-as-text, LIKE metacharacters, leading
-- and trailing spaces, the empty string.
CREATE TEMP TABLE words(k int, w text);
INSERT INTO words SELECT ordinality::int, w FROM unnest(ARRAY[
  'apple','Apple','APPLE','banana','Banana','cherry','Zebra','zebra',
  'éclair','Éclair','eclair','über','uber','Ünder','naïve','naive',
  'ñandú','nandu','日本語','中文','Ωmega','omega','straße','strasse','STRASSE',
  'a b','a_b','a-b','a%b','ab','aB','Ab','AB','','  lead','trail  ',
  'O''Brien','100','20','3','_under','[bracket]','back\slash','emoji😀',
  'İstanbul','istanbul','ß','ÿ','résumé','resume']) WITH ORDINALITY AS u(w, ordinality);

INSERT INTO t_str
SELECT g,
  CASE WHEN g % 14 = 0 THEN NULL
       ELSE (SELECT w FROM words WHERE k = 1 + (g * 7) % 50)
            || CASE WHEN g % 3 = 0 THEN '' ELSE '-' || (g % 40) END END,
  CASE WHEN g % 9 = 0 THEN NULL
       ELSE 'User ' || (g % 120) || CASE g % 4 WHEN 0 THEN '  ' ELSE '' END END,
  CASE WHEN g % 12 = 0 THEN NULL ELSE left((SELECT w FROM words WHERE k = 1 + (g * 3) % 50), 8) END,
  CASE WHEN g % 15 = 0 THEN NULL ELSE md5((g % 700)::text)::uuid END,
  CASE WHEN g % 10 = 0 THEN NULL ELSE (ARRAY['sad','ok','happy','ecstatic'])[1 + (g * 3) % 4]::mood END,
  CASE WHEN g % 16 = 0 THEN NULL
       WHEN g % 5 = 0 THEN ('2001:db8::' || to_hex(g))::inet
       WHEN g % 3 = 0 THEN ('10.' || g % 256 || '.' || (g * 7) % 256 || '.0/24')::inet
       ELSE ('192.168.' || g % 256 || '.' || (g * 7) % 256)::inet END,
  CASE WHEN g % 18 = 0 THEN NULL ELSE substr(decode(md5(g::text), 'hex'), 1, g % 9) END,
  (SELECT w FROM words WHERE k = 1 + g % 50)
FROM generate_series(1, 2000) g;

INSERT INTO t_time
SELECT g,
  CASE WHEN g % 13 = 0 THEN NULL ELSE date '1999-12-25' + (g * 13) % 12000 END,
  CASE WHEN g % 17 = 0 THEN NULL
       ELSE time '00:00' + ((g * 997) % 86400) * interval '1 second'
            + CASE WHEN g % 4 = 0 THEN (g % 1000) * interval '1 millisecond' ELSE interval '0' END END,
  CASE WHEN g % 19 = 0 THEN NULL
       ELSE timestamp '2000-01-01' + ((g::bigint * 100003) % 800000000) * interval '1 second'
            + CASE WHEN g % 5 = 0 THEN (g * 1001 % 1000000) * interval '1 microsecond' ELSE interval '0' END END,
  CASE WHEN g % 11 = 0 THEN NULL
       ELSE timestamptz '2015-01-01 00:00:00+00' + ((g::bigint * 7919) % 5500000) * interval '1 minute' END,
  CASE WHEN g % 21 = 0 THEN NULL
       ELSE make_interval(months => g % 30 - 5, days => g % 45 - 10, secs => (g * 37) % 100000 - 20000) END,
  CASE WHEN g % 23 = 0 THEN NULL
       ELSE ((time '00:00' + ((g * 613) % 86400) * interval '1 second')::text
             || (ARRAY['+00','+05:30','-08'])[1 + g % 3])::timetz END
FROM generate_series(1, 2000) g;

INSERT INTO t_doc
SELECT g,
  CASE WHEN g % 12 = 0 THEN NULL
       ELSE format('{"id": %s,  "name":"%s", "tags":["t%s","t%s"], "n": {"x": %s, "y": null}, "price": %s, "ok": %s}',
                   g, 'n' || g % 30, g % 5, g % 7, g % 9, (g % 1000) / 10.0, CASE WHEN g % 2 = 0 THEN 'true' ELSE 'false' END)::json END,
  CASE WHEN g % 10 = 0 THEN NULL
       ELSE jsonb_build_object('id', g, 'name', 'n' || g % 30, 'tags', jsonb_build_array('t' || g % 5, 't' || g % 7),
                               'n', jsonb_build_object('x', g % 9, 'y', NULL), 'price', (g % 1000) / 10.0,
                               'ok', g % 2 = 0, 'big', g::bigint * 1000000007, 'zz', 'Ünï-' || g % 4) END,
  CASE WHEN g % 14 = 0 THEN NULL WHEN g % 9 = 0 THEN '{}'::int[]
       ELSE ARRAY[g % 5, CASE WHEN g % 6 = 0 THEN NULL ELSE g % 7 END, -(g % 3)] END,
  CASE WHEN g % 15 = 0 THEN NULL
       ELSE ARRAY['t' || g % 5, CASE WHEN g % 4 = 0 THEN NULL ELSE 'x y' END, CASE WHEN g % 8 = 0 THEN '' ELSE 'q"' || g % 3 END] END,
  CASE WHEN g % 16 = 0 THEN NULL ELSE ARRAY[(g % 100) / 4.0, 1.5]::numeric(8,2)[] END
FROM generate_series(1, 1000) g;

INSERT INTO customers
SELECT g,
  (ARRAY['Ada','bob','Čapek','carol','Émile','eve','Zoë','zack','Ørsted','otto'])[1 + g % 10] || ' ' || g,
  CASE WHEN g % 9 = 0 THEN NULL ELSE (ARRAY['north','South','EAST','west','Île-de-France'])[1 + g % 5] END,
  CASE WHEN g % 8 = 0 THEN NULL ELSE (ARRAY['sad','ok','happy','ecstatic'])[1 + g % 4]::mood END,
  CASE WHEN g % 7 = 0 THEN NULL ELSE 'user' || g || '@' || (ARRAY['Example.com','example.org','test.io'])[1 + g % 3] END,
  date '2018-01-01' + (g * 11) % 2500
FROM generate_series(1, 300) g;

INSERT INTO orders
SELECT g,
  CASE WHEN g % 20 = 0 THEN NULL ELSE 1 + (g * 13) % 330 END,       -- 301..330 match no customer
  CASE WHEN g % 25 = 0 THEN NULL ELSE ((g * 7919) % 100000) / 100.0 END,
  CASE WHEN g % 30 = 0 THEN NULL ELSE (g * 7) % 13 - 2 END,
  CASE WHEN g % 22 = 0 THEN NULL ELSE (ARRAY['new','paid','shipped','cancelled','Paid'])[1 + (g * 3) % 5] END,
  CASE WHEN g % 27 = 0 THEN NULL
       ELSE timestamptz '2023-01-01 00:00:00+00' + ((g::bigint * 104729) % 1051200) * interval '1 minute' END,
  CASE WHEN g % 6 = 0 THEN 'gift' WHEN g % 10 = 0 THEN '' WHEN g % 4 = 0 THEN NULL ELSE 'note ' || g % 17 END
FROM generate_series(1, 3000) g;

-- The tricky t_str rows are loaded here, by snapshot, not with the other special
-- rows over the stream. The two paths store char(n) and inet DIFFERENTLY (see
-- probes.sql: probe_char8, probe_inet_host), qs-verify reports the streamed
-- form as DIVERGED, and this suite only measures a mirror qs-verify confirms.
INSERT INTO t_str (id, t, vc, ch, u, e, ip, raw, g) VALUES
 (9001, 'tab' || chr(9) || 'here', 'line' || chr(10) || 'break', 'x', '00000000-0000-0000-0000-000000000000', 'sad', '0.0.0.0', '\x', NULL),
 (9002, 'ǅemal', 'Ǆ', 'É', 'ffffffff-ffff-ffff-ffff-ffffffffffff', 'ecstatic', '255.255.255.255', '\x00', NULL),
 (9003, ' ', '', '', 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11', 'ok', '::1', '\x00ff00', NULL),
 (9004, 'NULL', 'null', 'NULL', NULL, 'happy', '192.168.1.5/24', '\xdeadbeef', NULL),
 (9005, 'e' || chr(769) || 'clair', 'ﬁne', 'ab  ', NULL, NULL, '10.0.0.0/8', '\x5c', NULL),
 (9006, 'a,b"c{d}', 'x''y', 'a\b', NULL, NULL, NULL, NULL, NULL),
 (9007, repeat('long', 200), 'ΑΒΓ αβγ', 'ÀÉÎÕÜ', NULL, NULL, NULL, NULL, NULL);
