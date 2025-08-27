-- minimal project tables (we’ll expand later)
create table if not exists locations(
  id serial primary key,
  name text not null,
  type text check (type in ('station','commune')) not null,
  lat double precision,
  lon double precision
);
-- ensure table exists (if you haven’t added it yet)
create table if not exists fact_mobility(
  id serial primary key,
  location_id int references locations(id),
  ts timestamptz not null,
  vehicle_count int,
  avg_speed_kmh numeric
);

-- idempotency key: one record per location+timestamp
alter table fact_mobility
  add constraint ux_fact_mobility unique(location_id, ts);

create index if not exists ix_fact_mobility_ts on fact_mobility(ts);


-- quick canary table to test writes
create table if not exists canary(
  ts timestamptz default now(),
  note text
);
