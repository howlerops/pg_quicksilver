-- setup for experiment b (primary, superuser): a schema of look-alike built-ins, owned by app.
-- Every function notes once per session (evil.noted, a custom setting) that it ran, and as whom,
-- so that one NOTICE shows that the evil code ran, and as which role, without a NOTICE per row.
\set VERBOSITY terse
CREATE SCHEMA evil;
GRANT USAGE, CREATE ON SCHEMA evil TO app;
SET ROLE app;
CREATE FUNCTION evil.note() RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF coalesce(current_setting('evil.noted', true), '') <> 'yes' THEN
    PERFORM set_config('evil.noted', 'yes', false);
    RAISE NOTICE 'evil ran as %', current_user;
  END IF;
END $$;
-- evil.sum(numeric): an aggregate whose answer is right, and which notes when it is finished
CREATE FUNCTION evil.sum_step(numeric, numeric) RETURNS numeric LANGUAGE sql IMMUTABLE AS 'SELECT $1 + $2';
CREATE FUNCTION evil.sum_final(numeric) RETURNS numeric LANGUAGE plpgsql AS $$ BEGIN PERFORM evil.note(); RETURN $1; END $$;
CREATE AGGREGATE evil.sum(numeric) (SFUNC = evil.sum_step, STYPE = numeric, INITCOND = '0', FINALFUNC = evil.sum_final);
-- evil.count(*)
CREATE FUNCTION evil.count_step(bigint) RETURNS bigint LANGUAGE sql IMMUTABLE AS 'SELECT $1 + 1';
CREATE FUNCTION evil.count_final(bigint) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN PERFORM evil.note(); RETURN $1; END $$;
CREATE AGGREGATE evil.count(*) (SFUNC = evil.count_step, STYPE = bigint, INITCOND = '0', FINALFUNC = evil.count_final);
-- evil.= on text: right answer, notes when it is called
CREATE FUNCTION evil.eq_note(text, text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN PERFORM evil.note(); RETURN $1 = $2; END $$;
CREATE OPERATOR evil.= (LEFTARG = text, RIGHTARG = text, FUNCTION = evil.eq_note, RESTRICT = eqsel, JOIN = eqjoinsel);
RESET ROLE;
-- The first eq_note compared with a bare "=", which, with evil first on the search path, is evil.=
-- itself: every row recursed ("stack depth limit exceeded"). It is replaced by one that names
-- pg_catalog's operator. Owner stays app.
CREATE OR REPLACE FUNCTION evil.eq_note(text, text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$ BEGIN PERFORM evil.note(); RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
SELECT n.nspname, p.proname, pg_get_userbyid(p.proowner) AS owner FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'evil' ORDER BY 2;
SELECT oprname, pg_get_userbyid(oprowner) AS owner FROM pg_operator WHERE oprnamespace = 'evil'::regnamespace;
