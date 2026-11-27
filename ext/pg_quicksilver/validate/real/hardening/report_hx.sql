-- run on the STANDBY as superuser: the sidecar's job (report the applied LSN) for the test table
-- public.hx, whose mirror is a view over a file this test controls. Reported ahead of the replay
-- position, as errors.sql does on the primary, so that the mirror counts as fresh.
SELECT quicksilver.report_applied_lsn('public.hx', pg_last_wal_replay_lsn() + 1000000000);
