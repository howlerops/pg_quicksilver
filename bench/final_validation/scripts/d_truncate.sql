-- Scenario d: TRUNCATE fv.events, then reload it with the same generator as fv_schema.sql (run on the primary).
TRUNCATE fv.events;
INSERT INTO fv.events
  SELECT md5('e' || i::text)::uuid,
         ((i * 7919) % 1000000)::numeric / 100 - 2000,
         timestamptz '2025-01-01 00:00:00+00' + (i * 97) * interval '1 minute' + (i % 1000) * interval '1 microsecond',
         CASE WHEN i % 11 = 0 THEN NULL ELSE 'note ' || (i % 5000) END
  FROM generate_series(1, 200000) AS i;
ANALYZE fv.events;
