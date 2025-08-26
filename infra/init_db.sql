-- Minimal schema for Step 1
create table if not exists locations(
  id serial primary key,
  name text not null,
  type text check (type in ('station','commune')) not null,
  lat double precision,
  lon double precision
);

create table if not exists fact_mobility(
  id bigserial primary key,
  location_id int references locations(id),
  ts timestamptz not null,
  vehicle_count int,
  avg_speed_kmh numeric(6,2)
);

create table if not exists fact_energy(
  id bigserial primary key,
  location_id int references locations(id),
  ts timestamptz not null,
  consumption_mwh numeric(10,2),
  price_eur_mwh numeric(10,2)
);

create table if not exists fact_weather(
  id bigserial primary key,
  location_id int references locations(id),
  ts timestamptz not null,
  temp_c numeric(5,2),
  precip_mm numeric(6,2)
);
