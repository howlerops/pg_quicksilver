-- quicksilver mirror public.customers applied_lsn=1/E00AF8C0
WITH qs_whole AS (
  SELECT "id", "tenant_id", "name", "email", "created_at" FROM read_parquet('/tmp/qsb/mirror/public.customers/base/000001.parquet')
)
SELECT qs_whole."id" AS "id",
       qs_whole."tenant_id" AS "tenant_id",
       qs_whole."name" AS "name",
       qs_whole."email" AS "email",
       qs_whole."created_at" AS "created_at"
FROM qs_whole
