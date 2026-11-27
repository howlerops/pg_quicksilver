-- quicksilver mirror public.orders applied_lsn=1/E00AF8C0
WITH qs_whole AS (
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/base/000001.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000001.000000004.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000018.parquet')
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000010.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000010.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000011.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000011.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000012.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000012.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000013.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000013.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000014.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000014.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000015.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000015.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000016.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000016.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000017.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000017.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000019.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000019.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000020.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000020.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000021.parquet', file_row_number = true) WHERE file_row_number NOT IN (SELECT p FROM read_parquet('/tmp/qsb/mirror/public.orders/dv/000021.000000001.dv.parquet'))
  UNION ALL
  SELECT "id", "tenant_id", "customer_id", "status", "amount", "created_at", "payload", "note" FROM read_parquet('/tmp/qsb/mirror/public.orders/delta/000022.parquet')
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
