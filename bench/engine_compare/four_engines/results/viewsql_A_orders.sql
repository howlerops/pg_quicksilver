-- quicksilver mirror public.orders applied_lsn=0/EF66AA78
WITH qs_whole AS (
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/base/000001.parquet')
)
SELECT qs_whole."id" AS "id",
       qs_whole."tenant_id" AS "tenant_id",
       qs_whole."customer_id" AS "customer_id",
       qs_whole."status" AS "status",
       qs_whole."amount" AS "amount",
       qs_whole."created_at" AS "created_at",
       qs_whole."payload" AS "payload",
       qs_whole."note" AS "note"
FROM qs_whole
