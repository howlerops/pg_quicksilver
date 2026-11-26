SELECT pid, usename, datname, state, left(query, 90) AS query, backend_start FROM pg_stat_activity WHERE pid <> pg_backend_pid() ORDER BY backend_start;
SELECT pg_current_wal_lsn() AS wal_now;
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;
