-- Item 1 tables: a uuid key, numeric and timestamptz columns, a wide text table, a small lookup.
CREATE TABLE ev_uuid (id uuid PRIMARY KEY, n int NOT NULL, payload text, updated timestamptz NOT NULL DEFAULT now());
CREATE TABLE ledger (id bigint PRIMARY KEY, amount numeric(14,3) NOT NULL, ts timestamptz NOT NULL, note text);
CREATE TABLE wide_text (id int PRIMARY KEY, c01 text, c02 text, c03 text, c04 text, c05 text, c06 text, c07 text, c08 text, c09 text, c10 text, c11 text, c12 text);
CREATE TABLE lookup (code int PRIMARY KEY, label text NOT NULL);
INSERT INTO ev_uuid SELECT gen_random_uuid(), g, 'seed'||g, now() FROM generate_series(1,2000) g;
INSERT INTO ledger SELECT g, (g*1.5)::numeric(14,3), now() - (g||' minutes')::interval, 'seed' FROM generate_series(1,2000) g;
INSERT INTO wide_text SELECT g, repeat('a',40)||g, repeat('b',40)||g, repeat('c',40)||g, repeat('d',40)||g, repeat('e',40)||g, repeat('f',40)||g, repeat('g',40)||g, repeat('h',40)||g, repeat('i',40)||g, repeat('j',40)||g, repeat('k',40)||g, repeat('l',40)||g FROM generate_series(1,2000) g;
INSERT INTO lookup SELECT g, 'label'||g FROM generate_series(1,50) g;
