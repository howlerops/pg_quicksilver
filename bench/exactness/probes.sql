-- One table per value the mirror may refuse or store differently. Each is
-- mirrored on its own, so a halt is attributable to exactly one value.
--
-- Row 1 is here and reaches the mirror by SNAPSHOT. If the mirror survives it,
-- the runner copies it to row 2 after the sidecar is up, so the same value also
-- arrives over the replication STREAM. For each probe the runner records:
-- halted / crashed / mirrored, what qs-verify said, and what DuckDB reads back
-- for each row next to what PostgreSQL prints. Results: results/probes.json.
--
-- None of these values is in the measured tables.
CREATE TABLE probe_numeric_nan      (id int PRIMARY KEY, v numeric);
CREATE TABLE probe_numeric_inf      (id int PRIMARY KEY, v numeric);
CREATE TABLE probe_numeric_scale15  (id int PRIMARY KEY, v numeric);
CREATE TABLE probe_numeric_30digits (id int PRIMARY KEY, v numeric);
CREATE TABLE probe_numeric_div      (id int PRIMARY KEY, v numeric);
CREATE TABLE probe_numeric_p50      (id int PRIMARY KEY, v numeric(50,2));
CREATE TABLE probe_date_bc          (id int PRIMARY KEY, v date);
CREATE TABLE probe_ts_bc            (id int PRIMARY KEY, v timestamp);
CREATE TABLE probe_ts_year294276    (id int PRIMARY KEY, v timestamp);
CREATE TABLE probe_date_year5m      (id int PRIMARY KEY, v date);
CREATE TABLE probe_tstz_year1_bc    (id int PRIMARY KEY, v timestamptz);
CREATE TABLE probe_time_24          (id int PRIMARY KEY, v time);
CREATE TABLE probe_float8_nan       (id int PRIMARY KEY, v double precision);
CREATE TABLE probe_float8_inf       (id int PRIMARY KEY, v double precision);
CREATE TABLE probe_float8_1e6       (id int PRIMARY KEY, v double precision);
CREATE TABLE probe_float8_9digits   (id int PRIMARY KEY, v double precision);
CREATE TABLE probe_real_inf         (id int PRIMARY KEY, v real);
CREATE TABLE probe_real_max         (id int PRIMARY KEY, v real);
CREATE TABLE probe_interval_big     (id int PRIMARY KEY, v interval);
CREATE TABLE probe_money            (id int PRIMARY KEY, v money);
CREATE TABLE probe_char8            (id int PRIMARY KEY, v char(8));
CREATE TABLE probe_varchar_trailing (id int PRIMARY KEY, v varchar(8));
CREATE TABLE probe_inet_host        (id int PRIMARY KEY, v inet);
CREATE TABLE probe_inet_masked      (id int PRIMARY KEY, v inet);
CREATE TABLE probe_timetz           (id int PRIMARY KEY, v timetz);
INSERT INTO probe_numeric_nan      VALUES (1, 'NaN');
INSERT INTO probe_numeric_inf      VALUES (1, 'Infinity');
INSERT INTO probe_numeric_scale15  VALUES (1, 0.123456789012345);
INSERT INTO probe_numeric_30digits VALUES (1, 123456789012345678901234567890);
INSERT INTO probe_numeric_div      VALUES (1, 1::numeric / 3);
INSERT INTO probe_numeric_p50      VALUES (1, 1.50);
INSERT INTO probe_date_bc          VALUES (1, '0044-03-15 BC');
INSERT INTO probe_ts_bc            VALUES (1, '0044-03-15 12:00:00 BC');
INSERT INTO probe_ts_year294276    VALUES (1, '294276-12-31 23:59:59');
INSERT INTO probe_date_year5m      VALUES (1, '5874897-12-31');
INSERT INTO probe_tstz_year1_bc    VALUES (1, '0001-01-01 00:00:00+14');
INSERT INTO probe_time_24          VALUES (1, '24:00:00');
INSERT INTO probe_float8_nan       VALUES (1, 'NaN');
INSERT INTO probe_float8_inf       VALUES (1, 'Infinity');
INSERT INTO probe_float8_1e6       VALUES (1, 1000000);
INSERT INTO probe_float8_9digits   VALUES (1, 123456789.12345679);
INSERT INTO probe_real_inf         VALUES (1, '-Infinity');
INSERT INTO probe_real_max         VALUES (1, 3.4028235e38);
INSERT INTO probe_interval_big     VALUES (1, '178000000 years');
INSERT INTO probe_money            VALUES (1, '12.34');
INSERT INTO probe_char8            VALUES (1, 'ab');
INSERT INTO probe_varchar_trailing VALUES (1, 'ab  ');
INSERT INTO probe_inet_host        VALUES (1, '192.168.1.7');
INSERT INTO probe_inet_masked      VALUES (1, '192.168.1.7/24');
INSERT INTO probe_timetz           VALUES (1, '12:30:00+05:30');
