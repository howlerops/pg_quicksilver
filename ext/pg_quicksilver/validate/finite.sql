-- Fills quicksilver.mirrors.finite_columns by looking at the heap. A stand-in:
-- in production this is the sidecar's to maintain, because it sees every value
-- it writes and a scan of the heap is only true for the moment it ran.
SET quicksilver.route = off;
DO $$
DECLARE
  m record; r record; ok boolean; cols name[];
BEGIN
  FOR m IN SELECT heap FROM quicksilver.mirrors LOOP
    cols := '{}';
    FOR r IN SELECT attname FROM pg_attribute
             WHERE attrelid = m.heap AND attnum > 0 AND NOT attisdropped
               AND atttypid IN ('date'::regtype, 'timestamp'::regtype, 'timestamptz'::regtype) LOOP
      EXECUTE format('SELECT coalesce(bool_and(isfinite(%I)), true) FROM %s', r.attname, m.heap) INTO ok;
      IF ok THEN cols := cols || r.attname; END IF;
    END LOOP;
    UPDATE quicksilver.mirrors SET finite_columns = cols WHERE heap = m.heap AND finite_columns IS DISTINCT FROM cols;
  END LOOP;
END $$;
