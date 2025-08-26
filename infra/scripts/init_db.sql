-- minimal project tables (we’ll expand later)
create table if not exists locations(
  id serial primary key,
  name text not null,
  type text check (type in ('station','commune')) not null,
  lat double precision,
  lon double precision
);

-- quick canary table to test writes
create table if not exists canary(
  ts timestamptz default now(),
  note text
);
