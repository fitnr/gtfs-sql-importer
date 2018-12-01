CREATE SCHEMA gtfs;
CREATE EXTENSION postgis;


DROP TABLE IF EXISTS gtfs.agency cascade;
DROP TABLE IF EXISTS gtfs.stops cascade;
DROP TABLE IF EXISTS gtfs.routes cascade;
DROP TABLE IF EXISTS gtfs.calendar cascade;
DROP TABLE IF EXISTS gtfs.calendar_dates cascade;
DROP TABLE IF EXISTS gtfs.fare_attributes cascade;
DROP TABLE IF EXISTS gtfs.fare_rules cascade;
DROP TABLE IF EXISTS gtfs.shapes cascade;
DROP TABLE IF EXISTS gtfs.trips cascade;
DROP TABLE IF EXISTS gtfs.stop_times cascade;
DROP TABLE IF EXISTS gtfs.frequencies cascade;
DROP TABLE IF EXISTS gtfs.shape_geoms CASCADE;
DROP TABLE IF EXISTS gtfs.transfers cascade;
DROP TABLE IF EXISTS gtfs.timepoints cascade;
DROP TABLE IF EXISTS gtfs.feed_info cascade;
DROP TABLE IF EXISTS gtfs.route_types cascade;
DROP TABLE IF EXISTS gtfs.pickup_dropoff_types cascade;
DROP TABLE IF EXISTS gtfs.payment_methods cascade;
DROP TABLE IF EXISTS gtfs.location_types cascade;
DROP TABLE IF EXISTS gtfs.exception_types cascade;
DROP TABLE IF EXISTS gtfs.wheelchair_boardings cascade;
DROP TABLE IF EXISTS gtfs.wheelchair_accessible cascade;
DROP TABLE IF EXISTS gtfs.transfer_types cascade;

BEGIN;

CREATE TABLE gtfs.feed_info (
  feed_index serial PRIMARY KEY, -- tracks uploads, avoids key collisions
  feed_publisher_name text DEFAULT NULL,
  feed_publisher_url text DEFAULT NULL,
  feed_timezone text DEFAULT NULL,
  feed_lang text DEFAULT NULL,
  feed_version text DEFAULT NULL,
  feed_start_date date DEFAULT NULL,
  feed_end_date date DEFAULT NULL,
  feed_id text DEFAULT NULL,
  feed_contact_url text DEFAULT NULL,
  feed_download_date date,
  feed_file text
);

CREATE TABLE gtfs.agency (
  feed_index integer REFERENCES gtfs.feed_info (feed_index),
  agency_id text DEFAULT '',
  agency_name text DEFAULT NULL,
  agency_url text DEFAULT NULL,
  agency_timezone text DEFAULT NULL,
  -- optional
  agency_lang text DEFAULT NULL,
  agency_phone text DEFAULT NULL,
  agency_fare_url text DEFAULT NULL,
  agency_email text DEFAULT NULL,
  bikes_policy_url text DEFAULT NULL,
  CONSTRAINT gtfs_agency_pkey PRIMARY KEY (feed_index, agency_id)
);

--related to gtfs.calendar_dates(exception_type)
CREATE TABLE gtfs.exception_types (
  exception_type int PRIMARY KEY,
  description text
);

--related to gtfs.stops(wheelchair_accessible)
CREATE TABLE gtfs.wheelchair_accessible (
  wheelchair_accessible int PRIMARY KEY,
  description text
);

--related to gtfs.stops(wheelchair_boarding)
CREATE TABLE gtfs.wheelchair_boardings (
  wheelchair_boarding int PRIMARY KEY,
  description text
);

CREATE TABLE gtfs.pickup_dropoff_types (
  type_id int PRIMARY KEY,
  description text
);

CREATE TABLE gtfs.transfer_types (
  transfer_type int PRIMARY KEY,
  description text
);

--related to gtfs.stops(location_type)
CREATE TABLE gtfs.location_types (
  location_type int PRIMARY KEY,
  description text
);

-- related to gtfs.stop_times(timepoint)
CREATE TABLE gtfs.timepoints (
  timepoint int PRIMARY KEY,
  description text
);

CREATE TABLE gtfs.calendar (
  feed_index integer NOT NULL,
  service_id text,
  monday int NOT NULL,
  tuesday int NOT NULL,
  wednesday int NOT NULL,
  thursday int NOT NULL,
  friday int NOT NULL,
  saturday int NOT NULL,
  sunday int NOT NULL,
  start_date date NOT NULL,
  end_date date NOT NULL,
  CONSTRAINT gtfs_calendar_pkey PRIMARY KEY (feed_index, service_id),
  CONSTRAINT gtfs_calendar_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);
CREATE INDEX gtfs_calendar_service_id ON gtfs.calendar (service_id);

CREATE TABLE gtfs.stops (
  feed_index int NOT NULL,
  stop_id text,
  stop_name text DEFAULT NULL,
  stop_desc text DEFAULT NULL,
  stop_lat double precision,
  stop_lon double precision,
  zone_id text,
  stop_url text,
  stop_code text,
  stop_street text,
  stop_city text,
  stop_region text,
  stop_postcode text,
  stop_country text,
  stop_timezone text,
  direction text,
  position text DEFAULT NULL,
  parent_station text DEFAULT NULL,
  wheelchair_boarding integer DEFAULT NULL REFERENCES gtfs.wheelchair_boardings (wheelchair_boarding),
  wheelchair_accessible integer DEFAULT NULL REFERENCES gtfs.wheelchair_accessible (wheelchair_accessible),
  -- optional
  location_type integer DEFAULT NULL REFERENCES gtfs.location_types (location_type),
  vehicle_type int DEFAULT NULL,
  platform_code text DEFAULT NULL,
  CONSTRAINT gtfs_stops_pkey PRIMARY KEY (feed_index, stop_id)
);
SELECT AddGeometryColumn('gtfs', 'stops', 'the_geom', 4326, 'POINT', 2);

-- trigger the_geom update with lat or lon inserted
CREATE OR REPLACE FUNCTION gtfs.stop_geom_update() RETURNS TRIGGER AS $stop_geom$
  BEGIN
    NEW.the_geom = ST_SetSRID(ST_MakePoint(NEW.stop_lon, NEW.stop_lat), 4326);
    RETURN NEW;
  END;
$stop_geom$ LANGUAGE plpgsql;

CREATE TRIGGER stop_geom_trigger BEFORE INSERT OR UPDATE ON gtfs.stops
    FOR EACH ROW EXECUTE PROCEDURE gtfs.stop_geom_update();

CREATE TABLE gtfs.route_types (
  route_type int PRIMARY KEY,
  description text
);

CREATE TABLE gtfs.routes (
  feed_index int NOT NULL,
  route_id text,
  agency_id text,
  route_short_name text DEFAULT '',
  route_long_name text DEFAULT '',
  route_desc text DEFAULT '',
  route_type int REFERENCES gtfs.route_types(route_type),
  route_url text,
  route_color text,
  route_text_color text,
  -- unofficial
  route_sort_order integer DEFAULT NULL,
  CONSTRAINT gtfs_routes_pkey PRIMARY KEY (feed_index, route_id),
  -- CONSTRAINT gtfs_routes_fkey FOREIGN KEY (feed_index, agency_id)
  --   REFERENCES gtfs.agency (feed_index, agency_id),
  CONSTRAINT gtfs_routes_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs.calendar_dates (
  feed_index int NOT NULL,
  service_id text,
  date date NOT NULL,
  exception_type int REFERENCES gtfs.exception_types(exception_type) --,
  -- CONSTRAINT gtfs_calendar_fkey FOREIGN KEY (feed_index, service_id)
    -- REFERENCES gtfs.calendar (feed_index, service_id)
);

CREATE INDEX gtfs_calendar_dates_dateidx ON gtfs.calendar_dates (date);

CREATE TABLE gtfs.payment_methods (
  payment_method int PRIMARY KEY,
  description text
);

CREATE TABLE gtfs.fare_attributes (
  feed_index int NOT NULL,
  fare_id text NOT NULL,
  price double precision NOT NULL,
  currency_type text NOT NULL,
  payment_method int REFERENCES gtfs.payment_methods,
  transfers int,
  transfer_duration int,
  -- unofficial features
  agency_id text DEFAULT NULL,
  CONSTRAINT gtfs_fare_attributes_pkey PRIMARY KEY (feed_index, fare_id),
  -- CONSTRAINT gtfs_fare_attributes_fkey FOREIGN KEY (feed_index, agency_id)
  -- REFERENCES gtfs.agency (feed_index, agency_id),
  CONSTRAINT gtfs_fare_attributes_fare_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs.fare_rules (
  feed_index int NOT NULL,
  fare_id text,
  route_id text,
  origin_id text,
  destination_id text,
  contains_id text,
  -- unofficial features
  service_id text DEFAULT NULL,
  -- CONSTRAINT gtfs_fare_rules_service_fkey FOREIGN KEY (feed_index, service_id)
  -- REFERENCES gtfs.calendar (feed_index, service_id),
  -- CONSTRAINT gtfs_fare_rules_fare_id_fkey FOREIGN KEY (feed_index, fare_id)
  -- REFERENCES gtfs.fare_attributes (feed_index, fare_id),
  -- CONSTRAINT gtfs_fare_rules_route_id_fkey FOREIGN KEY (feed_index, route_id)
  -- REFERENCES gtfs.routes (feed_index, route_id),
  CONSTRAINT gtfs_fare_rules_service_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs.shapes (
  feed_index int NOT NULL,
  shape_id text NOT NULL,
  shape_pt_lat double precision NOT NULL,
  shape_pt_lon double precision NOT NULL,
  shape_pt_sequence int NOT NULL,
  -- optional
  shape_dist_traveled double precision DEFAULT NULL
);

CREATE INDEX gtfs_shapes_shape_key ON gtfs.shapes (shape_id);

CREATE OR REPLACE FUNCTION gtfs.shape_update()
  RETURNS TRIGGER AS $shape_update$
  BEGIN
    INSERT INTO gtfs.shape_geoms
      (feed_index, shape_id, length, the_geom)
    SELECT
      feed_index,
      shape_id,
      ST_Length(ST_MakeLine(array_agg(geom ORDER BY shape_pt_sequence))::geography) as length,
      ST_SetSRID(ST_MakeLine(array_agg(geom ORDER BY shape_pt_sequence)), 4326) AS the_geom
    FROM (
      SELECT
        feed_index,
        shape_id,
        shape_pt_sequence,
        ST_MakePoint(shape_pt_lon, shape_pt_lat) AS geom
      FROM gtfs.shapes s
        LEFT JOIN gtfs.shape_geoms sg USING (feed_index, shape_id)
      WHERE the_geom IS NULL
    ) a GROUP BY feed_index, shape_id;
  RETURN NULL;
  END;
$shape_update$ LANGUAGE plpgsql;

CREATE TRIGGER shape_geom_trigger AFTER INSERT ON gtfs.shapes
  FOR EACH STATEMENT EXECUTE PROCEDURE gtfs.shape_update();

-- Create new table to store the shape geometries
CREATE TABLE gtfs.shape_geoms (
  feed_index int NOT NULL,
  shape_id text NOT NULL,
  length numeric(12, 2) NOT NULL,
  CONSTRAINT gtfs_shape_geom_pkey PRIMARY KEY (feed_index, shape_id)
);
-- Add the_geom column to the gtfs.shape_geoms table - a 2D linestring geometry
SELECT AddGeometryColumn('gtfs', 'shape_geoms', 'the_geom', 4326, 'LINESTRING', 2);

CREATE TABLE gtfs.trips (
  feed_index int NOT NULL,
  route_id text NOT NULL,
  service_id text NOT NULL,
  trip_id text NOT NULL,
  trip_headsign text,
  direction_id int,
  block_id text,
  shape_id text,
  trip_short_name text,
  wheelchair_accessible int REFERENCES gtfs.wheelchair_accessible(wheelchair_accessible),

  -- unofficial features
  direction text DEFAULT NULL,
  schd_trip_id text DEFAULT NULL,
  trip_type text DEFAULT NULL,
  exceptional int DEFAULT NULL,
  bikes_allowed int DEFAULT NULL,
  CONSTRAINT gtfs_trips_pkey PRIMARY KEY (feed_index, trip_id),
  -- CONSTRAINT gtfs_trips_route_id_fkey FOREIGN KEY (feed_index, route_id)
  -- REFERENCES gtfs.routes (feed_index, route_id),
  -- CONSTRAINT gtfs_trips_calendar_fkey FOREIGN KEY (feed_index, service_id)
  -- REFERENCES gtfs.calendar (feed_index, service_id),
  CONSTRAINT gtfs_trips_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

CREATE INDEX gtfs_trips_trip_id ON gtfs.trips (trip_id);
CREATE INDEX gtfs_trips_service_id ON gtfs.trips (feed_index, service_id);



CREATE TABLE gtfs.stop_times (
  feed_index int NOT NULL,
  trip_id text NOT NULL,
  -- Check that casting to time interval works.
  arrival_time interval CHECK (arrival_time::interval = arrival_time::interval),
  departure_time interval CHECK (departure_time::interval = departure_time::interval),
  stop_id text,
  stop_sequence int NOT NULL,
  stop_headsign text,
  pickup_type int REFERENCES gtfs.pickup_dropoff_types(type_id),
  drop_off_type int REFERENCES gtfs.pickup_dropoff_types(type_id),
  shape_dist_traveled numeric(10, 2),
  timepoint int REFERENCES gtfs.timepoints (timepoint),

  -- unofficial features
  -- the following are not in the spec
  continuous_drop_off int DEFAULT NULL,
  continuous_pickup  int DEFAULT NULL,
  arrival_time_seconds int DEFAULT NULL,
  departure_time_seconds int DEFAULT NULL,
  CONSTRAINT gtfs_stop_times_pkey PRIMARY KEY (feed_index, trip_id, stop_sequence),
  -- CONSTRAINT gtfs_stop_times_trips_fkey FOREIGN KEY (feed_index, trip_id)
  -- REFERENCES gtfs.trips (feed_index, trip_id),
  -- CONSTRAINT gtfs_stop_times_stops_fkey FOREIGN KEY (feed_index, stop_id)
  -- REFERENCES gtfs.stops (feed_index, stop_id),
  CONSTRAINT gtfs_stop_times_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);
CREATE INDEX gtfs_stop_times_key ON gtfs.stop_times (feed_index, trip_id, stop_id);
CREATE INDEX arr_time_index ON gtfs.stop_times (arrival_time_seconds);
CREATE INDEX dep_time_index ON gtfs.stop_times (departure_time_seconds);

-- Insert dummy entires to 'calendars' if 'calendar_dates' is present
CREATE OR REPLACE FUNCTION gtfs.calendar_insert()
  RETURNS trigger AS
  $BODY$
  BEGIN
    INSERT INTO gtfs.calendar VALUES (NEW.feed_index, NEW.service_id, 0, 0, 0, 0, 0, 0, 0, '1970-01-01', '2099-12-31')
      ON CONFLICT DO NOTHING;
  RETURN NEW;
  END;
  $BODY$
LANGUAGE plpgsql;



-- "Safely" locate a point on a (possibly complicated) line by using minimum and maximum distances.
-- Unlike st_LineLocatePoint, this accepts and returns absolute distances, not fractions
CREATE OR REPLACE FUNCTION safe_locate
  (route geometry, point geometry, start numeric, finish numeric, length numeric)
  RETURNS numeric AS $$
    -- Multiply the fractional distance also the substring by the substring,
    -- then add the start distance
    SELECT LEAST(length, GREATEST(0, start) + ST_LineLocatePoint(
      ST_LineSubstring(route, GREATEST(0, start / length), LEAST(1, finish / length)),
      point
    )::numeric * (
      -- The absolute distance between start and finish
      LEAST(length, finish) - GREATEST(0, start)
    ));
  $$ LANGUAGE SQL;

-- Fill in the shape_dist_traveled field using stop and shape geometries. 
CREATE OR REPLACE FUNCTION gtfs.dist_insert()
  RETURNS TRIGGER AS $$
  BEGIN
  NEW.shape_dist_traveled := (
    SELECT
      ST_LineLocatePoint(route.the_geom, stop.the_geom) * route.length
    FROM gtfs.stops as stop
      LEFT JOIN gtfs.trips ON (stop.feed_index=trips.feed_index AND trip_id=NEW.trip_id)
      LEFT JOIN gtfs.shape_geoms AS route ON (route.feed_index = stop.feed_index and trips.shape_id = route.shape_id)
      WHERE stop_id = NEW.stop_id
        AND stop.feed_index = COALESCE(NEW.feed_index::integer, (
          SELECT column_DEFAULT::integer
          FROM information_schema.columns
          WHERE (table_schema, table_name, column_name) = ('gtfs', 'stop_times', 'feed_index')
        ))
  )::NUMERIC;
  RETURN NEW;
  END;
  $$
LANGUAGE plpgsql;

CREATE TRIGGER stop_times_dist_row_trigger BEFORE INSERT ON gtfs.stop_times
  FOR EACH ROW
  WHEN (NEW.shape_dist_traveled IS NULL)
  EXECUTE PROCEDURE gtfs.dist_insert();

-- Correct out-of-order shape_dist_traveled fields.
CREATE OR REPLACE FUNCTION gtfs.dist_update()
  RETURNS TRIGGER AS $$
  BEGIN
  WITH f AS (SELECT MAX(feed_index) AS feed_index FROM gtfs.feed_info),
  d as (
    SELECT
      feed_index,
      trip_id,
      stop_id,
      coalesce(lag(shape_dist_traveled) over (trip), 0) AS lag,
      shape_dist_traveled AS dist,
      lead(shape_dist_traveled) over (trip) AS lead
    FROM gtfs.stop_times
      INNER JOIN f USING (feed_index)
    WINDOW trip AS (PARTITION BY feed_index, trip_id ORDER BY stop_sequence)
  )
  UPDATE gtfs.stop_times s
    SET shape_dist_traveled = safe_locate(r.the_geom, p.the_geom, lag::numeric, coalesce(lead, length)::numeric, length::numeric)
  FROM d
    LEFT JOIN gtfs.stops p USING (feed_index, stop_id)
    LEFT JOIN gtfs.trips USING (feed_index, trip_id)
    LEFT JOIN gtfs.shape_geoms r USING (feed_index, shape_id)
  WHERE
      (s.feed_index, s.trip_id, s.stop_id) = (d.feed_index, d.trip_id, d.stop_id)
      AND COALESCE(lead, length) > lag
      AND (dist > COALESCE(lead, length) OR dist < lag);
  RETURN NULL;
  END;
  $$
LANGUAGE plpgsql;

CREATE TRIGGER stop_times_dist_stmt_trigger AFTER INSERT ON gtfs.stop_times
  FOR EACH STATEMENT EXECUTE PROCEDURE gtfs.dist_update();

CREATE TABLE gtfs.frequencies (
  feed_index int NOT NULL,
  trip_id text,
  start_time text NOT NULL CHECK (start_time::interval = start_time::interval),
  end_time text NOT NULL CHECK (end_time::interval = end_time::interval),
  headway_secs int NOT NULL,
  exact_times int,
  start_time_seconds int,
  end_time_seconds int,
  CONSTRAINT gtfs_frequencies_pkey PRIMARY KEY (feed_index, trip_id, start_time),
  -- CONSTRAINT gtfs_frequencies_trip_fkey FOREIGN KEY (feed_index, trip_id)
  --  REFERENCES gtfs.trips (feed_index, trip_id),
  CONSTRAINT gtfs_frequencies_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

CREATE TABLE gtfs.transfers (
  feed_index int NOT NULL,
  from_stop_id text,
  to_stop_id text,
  transfer_type int REFERENCES gtfs.transfer_types(transfer_type),
  min_transfer_time int,
  -- Unofficial fields
  from_route_id text DEFAULT NULL,
  to_route_id text DEFAULT NULL,
  service_id text DEFAULT NULL,
  -- CONSTRAINT gtfs_transfers_from_stop_fkey FOREIGN KEY (feed_index, from_stop_id)
  --  REFERENCES gtfs.stops (feed_index, stop_id),
  --CONSTRAINT gtfs_transfers_to_stop_fkey FOREIGN KEY (feed_index, to_stop_id)
  --  REFERENCES gtfs.stops (feed_index, stop_id),
  --CONSTRAINT gtfs_transfers_from_route_fkey FOREIGN KEY (feed_index, from_route_id)
  --  REFERENCES gtfs.routes (feed_index, route_id),
  --CONSTRAINT gtfs_transfers_to_route_fkey FOREIGN KEY (feed_index, to_route_id)
  --  REFERENCES gtfs.routes (feed_index, route_id),
  --CONSTRAINT gtfs_transfers_service_fkey FOREIGN KEY (feed_index, service_id)
  --  REFERENCES gtfs.calendar (feed_index, service_id),
  CONSTRAINT gtfs_transfers_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

insert into gtfs.exception_types (exception_type, description) values 
  (1, 'service has been added'),
  (2, 'service has been removed');

insert into gtfs.transfer_types (transfer_type, description) VALUES
  (0,'Preferred transfer point'),
  (1,'Designated transfer point'),
  (2,'Transfer possible with min_transfer_time window'),
  (3,'Transfers forbidden');

insert into gtfs.location_types(location_type, description) values 
  (0,'stop'),
  (1,'station'),
  (2,'station entrance');

insert into gtfs.wheelchair_boardings(wheelchair_boarding, description) values
   (0, 'No accessibility information available for the stop'),
   (1, 'At least some vehicles at this stop can be boarded by a rider in a wheelchair'),
   (2, 'Wheelchair boarding is not possible at this stop');

insert into gtfs.wheelchair_accessible(wheelchair_accessible, description) values
  (0, 'No accessibility information available for this trip'),
  (1, 'The vehicle being used on this particular trip can accommodate at least one rider in a wheelchair'),
  (2, 'No riders in wheelchairs can be accommodated on this trip');

insert into gtfs.pickup_dropoff_types (type_id, description) values
  (0,'Regularly Scheduled'),
  (1,'Not available'),
  (2,'Phone arrangement only'),
  (3,'Driver arrangement only');

insert into gtfs.payment_methods (payment_method, description) values
  (0,'On Board'),
  (1,'Prepay');

insert into gtfs.timepoints (timepoint, description) values
  (0, 'Times are considered approximate'),
  (1, 'Times are considered exact');

COMMIT;
