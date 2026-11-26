-- The join-domain tables of the equality corpus (corpus_join.sql), loaded when setup.sh runs
-- with JOIN_DOMAINS=on; the sidecar is given QS_JOIN_DOMAINS='cust=ex.jcust.id,ex.jord.cust'.
-- Keys: 12 customers, of which 4 have no order; 14 is an order key with no customer; one
-- order has a NULL key; ex.jord.alt holds the same uuids as cust in a column of no domain.
CREATE TABLE ex.jcust (id uuid PRIMARY KEY, region text COLLATE "C" NOT NULL, tier int NOT NULL);
INSERT INTO ex.jcust SELECT md5('jc' || g)::uuid, CASE g % 3 WHEN 0 THEN 'east' WHEN 1 THEN 'west' ELSE 'north' END,
  1 + g % 3 FROM generate_series(1, 12) g;
CREATE TABLE ex.jord (id int PRIMARY KEY, cust uuid, alt uuid, amount numeric(12,2) NOT NULL, qty int NOT NULL, note text);
INSERT INTO ex.jord SELECT g,
  CASE WHEN g % 11 = 0 THEN NULL ELSE md5('jc' || (g % 14))::uuid END,
  CASE WHEN g % 11 = 0 THEN NULL ELSE md5('jc' || (g % 14))::uuid END,
  round((g * 7.25)::numeric, 2), 1 + g % 5, CASE WHEN g % 4 = 0 THEN NULL ELSE 'n' || (g % 6) END
FROM generate_series(1, 30) g;
VACUUM ANALYZE ex.jcust;
VACUUM ANALYZE ex.jord;
