-- fix_b.sql, primary, superuser: the corrected evil.eq_note (see setup_b.sql, the note above it).
-- Run once after setup_b.sql, which ran its first version of this function.
CREATE OR REPLACE FUNCTION evil.eq_note(text, text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN PERFORM evil.note(); RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
