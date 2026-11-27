-- Source schema for the exactness suite. Every table's first column is its
-- primary key, which is what the mirror keys rows on.
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', 'ecstatic');   -- NOT alphabetical

CREATE TABLE t_num (
  id  int PRIMARY KEY,
  si  smallint,
  i   int,
  bi  bigint,
  n   numeric(12,2),
  n0  numeric(10,0),
  nu  numeric,              -- unconstrained: the mirror stores DECIMAL(38,9)
  r   real,
  d   double precision,
  b   boolean,
  g   int                   -- group key 0..9; NULL on the special rows
);

CREATE TABLE t_str (
  id  int PRIMARY KEY,
  t   text,
  vc  varchar(40),
  ch  char(8),
  u   uuid,
  e   mood,
  ip  inet,
  raw bytea,
  g   text                  -- a low-cardinality text group key
);

CREATE TABLE t_time (
  id  int PRIMARY KEY,
  d   date,
  tm  time,
  ts  timestamp,
  tz  timestamptz,
  iv  interval,
  ttz timetz
);

CREATE TABLE t_doc (
  id  int PRIMARY KEY,
  j   json,
  jb  jsonb,
  ia  int[],
  ta  text[],
  na  numeric(8,2)[]
);

CREATE TABLE customers (
  id      int PRIMARY KEY,
  name    text NOT NULL,
  region  text,
  tier    mood,
  email   varchar(80),
  created date
);

CREATE TABLE orders (
  id      bigint PRIMARY KEY,
  cust_id int,              -- nullable, and some point at no customer
  amount  numeric(10,2),
  qty     int,
  status  text,
  placed  timestamptz,
  note    text
);
