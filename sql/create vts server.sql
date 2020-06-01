CREATE EXTENSION dblink;
CREATE EXTENSION postgres_fdw;
-- Enable PostGIS (as of 3.0 contains just geometry/geography)
CREATE EXTENSION postgis;
-- enable raster support (for 3+)
CREATE EXTENSION postgis_raster;
-- Enable Topology
CREATE EXTENSION postgis_topology;
-- fuzzy matching needed for Tiger
CREATE EXTENSION fuzzystrmatch;
-- rule based standardizer
CREATE EXTENSION address_standardizer;
-- example rule data set
CREATE EXTENSION address_standardizer_data_us;
-- Enable US Tiger Geocoder
CREATE EXTENSION postgis_tiger_geocoder;

/*
 Test connections using dblink
 /*SELECT inet_server_addr();

prod-pg1-1.c7uhqtbvi1w1.us-east-1.rds.amazonaws.com
172.31.0.61
*/
SELECT * FROM
dblink ('dbname = agmri port = 5432 host = 172.31.0.61 user = pgadmin
password = p@ssword', 'SELECT id::text,
oi_id::bigint,
	field_id::bigint,
	farm_id::bigint,
	division_id::bigint,
	company_id::bigint,
	user_obj_id::bigint,
	mask::integer,
	user_id::bigint,
	owner_user_id::bigint
 FROM agmri.acl')
AS newTable(
id text,
oi_id bigint,
	field_id bigint,
	farm_id bigint,
	division_id bigint,
	company_id bigint,
	user_obj_id bigint,
	mask integer,
	user_id bigint,
	owner_user_id bigint
) ;
 */

drop foreign table if exists table_foreign;
DROP MATERIALIZED VIEW if exists vts.acl;
drop foreign table if exists vts.acl_ft;
drop VIEW if exists vts.fields_by_flight_ext;
drop materialized view if exists vts.fields_by_flight;
drop foreign table if exists vts.ft_field_by_flight;
drop foreign table if exists vts.ft_fields_by_flight;
drop VIEW if exists vts.fields_by_latestflight_ext;
drop materialized view if exists vts.fields_by_latestflight;
drop foreign table if exists vts.ft_fields_by_latestflight;
drop user mapping if exists for current_user server agmri;
drop server if exists agmri;

CREATE SERVER agmri FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '172.31.3.155',
    dbname 'agmri', port '5432');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER agmri
    OPTIONS (user 'pgadmin', password 'ynD26DQ1Njed596lMKKU');

CREATE FOREIGN TABLE vts.acl_ft (
    id text,
    oi_id bigint,
    field_id bigint,
    farm_id bigint,
    division_id bigint,
    company_id bigint,
    user_obj_id bigint,
    mask integer,
    user_id bigint,
    owner_user_id bigint
    )
    SERVER agmri OPTIONS (schema_name 'agmri', table_name 'acl');
-- View: vts.acl

-- DROP MATERIALIZED VIEW vts.acl;

CREATE MATERIALIZED VIEW vts.acl
    TABLESPACE pg_default
AS
SELECT distinct acl_ft.field_id,
                acl_ft.user_id
FROM vts.acl_ft
where field_id is not null
WITH DATA;

ALTER TABLE vts.acl
    OWNER TO root;
CREATE INDEX acl_field
    ON vts.acl USING btree
        (field_id)
    TABLESPACE pg_default;
CREATE INDEX acl_user
    ON vts.acl USING btree
        (user_id)
    TABLESPACE pg_default;

CREATE FOREIGN TABLE vts.ft_fields_by_flight (
    fieldid bigint,
    fieldtoken character varying(255),
    fieldprecipitation double precision,
    fieldprecipitation48 double precision,
    fieldprecipitation72 double precision,
    fieldprecipitationtotal double precision,
    fieldplantedseeds integer,
    fieldarea numeric,
    fieldthumbnailpath character varying(255),
    fieldisalert boolean,
    flight_date timestamp without time zone,
    flight_id bigint,
    flight_thermal_variance real,
    flight_thermal_mean real,
    flight_thermal_max real,
    flight_thermal_min real,
    flight_code character varying(255),
    flight_scores json,
    geometry geometry(Multipolygon, 4326),
    crop_types text,
    flight_number integer,
    features json
    )
    SERVER agmri OPTIONS (schema_name 'public', table_name 'fields_by_flight');

create materialized view vts.fields_by_flight
    TABLESPACE pg_default
AS
select row_number() OVER ()                                       AS "id",
       st_transform(geometry, 3857)::geometry(Multipolygon, 3857) as geometry,
       fieldid,
       fieldtoken,
       fieldprecipitation,
       fieldprecipitation48,
       fieldprecipitation72,
       fieldprecipitationtotal,
       fieldplantedseeds,
       fieldarea,
       fieldthumbnailpath,
       fieldisalert,
       flight_id,
       flight_date,
       flight_thermal_variance,
       flight_thermal_mean,
       flight_thermal_max,
       flight_thermal_min,
       flight_code,
       flight_scores,
       crop_types,
       flight_number,
       features
from vts.ft_fields_by_flight
WITH DATA;

ALTER TABLE vts.fields_by_flight
    OWNER TO root;
CREATE UNIQUE INDEX fbf_id
    ON vts.fields_by_flight USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
CREATE INDEX fbf_flight_id
    ON vts.fields_by_flight USING btree
        (flight_id)
    TABLESPACE pg_default;
CREATE INDEX fbf_flight_number
    ON vts.fields_by_flight USING btree
        (flight_number)
    TABLESPACE pg_default;
CREATE INDEX fbf_field_id
    ON vts.fields_by_flight USING btree
        (fieldid)
    TABLESPACE pg_default;

-- View: vts.fields_by_flight_ext

-- DROP VIEW vts.fields_by_flight_ext;

CREATE OR REPLACE VIEW vts.fields_by_flight_ext
AS
SELECT row_number() OVER ()                                                                                           AS "id",
       fl.geometry::geometry(Multipolygon, 3857),
       fl.fieldid,
       fl.fieldtoken,
       fl.fieldprecipitation,
       fl.fieldprecipitation48,
       fl.fieldprecipitation72,
       fl.fieldprecipitationtotal,
       fl.fieldplantedseeds,
       fl.fieldarea,
       fl.fieldthumbnailpath,
       fl.fieldisalert,
       fl.flight_date,
       fl.flight_id,
       fl.flight_thermal_variance,
       fl.flight_thermal_mean,
       fl.flight_thermal_max,
       fl.flight_thermal_min,
       fl.flight_code,
       fl.flight_scores,
       fl.crop_types,
       fl.flight_number,
       fl.features,
       case
           when (fl.features ->> 'field_harvested')::boolean
               then case when flight_number < 6 then 'Not Planted' else 'Harvested' end
           when (fl.features ->> 'field_emerged')::boolean then 'Emerged'
           when (fl.features ->> 'field_canopy_closed')::boolean then 'Canopy Closed'
           when (fl.features ->> 'field_planted')::boolean then 'Planted'
           when (fl.features ->> 'field_not_planted')::boolean then 'Not Planted'
           else 'Not Planted' end                                                                                     as field_state,
       (fl.features ->> 'field_crop_type')                                                                               field_crop_type,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'rowTracerPercent'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_percent,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'rowTracer'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_score,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'standCount'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_standCount,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'rank'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_rank,
       round((((fl.flight_scores -> 'HEAT_SEEKER'::text) ->> 'temp_mean'::text)::real)::numeric,
             2)::double precision                                                                                     AS heatSeeker_temp_mean,
       round((((fl.flight_scores -> 'HEAT_SEEKER'::text) ->> 'temp_variance'::text)::real)::numeric,
             2)::double precision                                                                                     AS heatSeeker_temp_variance,
       round((((fl.flight_scores -> 'WEED_WATCH'::text) ->> 'area'::text)::real)::numeric,
             2)::double precision                                                                                     AS weedWatch_area,
       round((((fl.flight_scores -> 'WEED_WATCH'::text) ->> 'pct'::text)::real)::numeric,
             2)::double precision                                                                                     AS weedWatch_percent,
       round((((fl.flight_scores -> 'WEED_WATCH'::text) ->> 'rank'::text)::real)::numeric,
             2)::double precision                                                                                     AS weedWatch_rank,
       round((((fl.flight_scores -> 'TREND_ZONE'::text) ->> 'area'::text)::real)::numeric,
             2)::double precision                                                                                     AS trendzone_area,
       round((((fl.flight_scores -> 'TREND_ZONE'::text) ->> 'pct'::text)::real)::numeric,
             2)::double precision                                                                                     AS trendzone_percent,
       round((((fl.flight_scores -> 'YIELD_RISK'::text) ->> 'area'::text)::real)::numeric,
             2)::double precision                                                                                     AS yieldrisk_area,
       round((((fl.flight_scores -> 'YIELD_RISK'::text) ->> 'pct'::text)::real)::numeric,
             2)::double precision                                                                                     AS yieldrisk_percent
FROM vts.fields_by_flight fl;

ALTER TABLE vts.fields_by_flight_ext
    OWNER TO root;

-- refresh materialized view vts.fields_by_flight

drop table if exists vts.test;
select id,
       geometry::geometry(Multipolygon, 3857),
       fieldid,
       fieldtoken,
       fieldprecipitation,
       fieldprecipitation48,
       fieldprecipitation72,
       fieldprecipitationtotal,
       fieldplantedseeds,
       fieldarea,
       fieldthumbnailpath,
       fieldisalert,
       flight_date,
       flight_id,
       flight_thermal_variance,
       flight_thermal_mean,
       flight_thermal_max,
       flight_thermal_min,
       flight_code,
       flight_scores,
       crop_types,
       flight_number,
       features,
       field_state,
       field_crop_type,
       rowtracer_percent,
       rowtracer_score,
       rowtracer_standcount,
       rowtracer_rank,
       heatseeker_temp_mean,
       heatseeker_temp_variance,
       weedwatch_area,
       weedwatch_percent,
       weedwatch_rank,
       trendzone_area,
       trendzone_percent,
       yieldrisk_area,
       yieldrisk_percent
into vts.test
from vts.fields_by_flight_ext f;

ALTER TABLE vts.test
    ADD CONSTRAINT test_id_contraint UNIQUE (id);
CREATE INDEX test_flight_id
    ON vts.test USING btree
        (flight_id)
    TABLESPACE pg_default;
CREATE INDEX test_flight_number
    ON vts.test USING btree
        (flight_number)
    TABLESPACE pg_default;
CREATE INDEX test_field_id
    ON vts.test USING btree
        (fieldid)
    TABLESPACE pg_default;
CREATE INDEX test_flight_number_field
    ON vts.test USING btree
        (flight_number, fieldid)
    TABLESPACE pg_default;


CREATE FOREIGN TABLE vts.ft_fields_by_latestflight (
    fieldid bigint,
    fieldtoken character varying(255),
    fieldprecipitation double precision,
    fieldprecipitation48 double precision,
    fieldprecipitation72 double precision,
    fieldprecipitationtotal double precision,
    fieldplantedseeds integer,
    fieldarea numeric,
    fieldthumbnailpath character varying(255),
    fieldisalert boolean,
    flight_date timestamp without time zone,
    flight_id bigint,
    flight_thermal_variance real,
    flight_thermal_mean real,
    flight_thermal_max real,
    flight_thermal_min real,
    flight_code character varying(255),
    flight_scores json,
    geometry geometry(Multipolygon, 4326),
    crop_types text,
    flight_number integer,
    features json
    )
    SERVER agmri OPTIONS (schema_name 'public', table_name 'fields_by_latestflight');

create materialized view vts.fields_by_latestflight
    TABLESPACE pg_default
AS
select row_number() OVER ()                                       AS "id",
       st_transform(geometry, 3857)::geometry(Multipolygon, 3857) as geometry,
       fieldid,
       fieldtoken,
       fieldprecipitation,
       fieldprecipitation48,
       fieldprecipitation72,
       fieldprecipitationtotal,
       fieldplantedseeds,
       fieldarea,
       fieldthumbnailpath,
       fieldisalert,
       flight_id,
       flight_date,
       flight_thermal_variance,
       flight_thermal_mean,
       flight_thermal_max,
       flight_thermal_min,
       flight_code,
       flight_scores,
       crop_types,
       flight_number,
       features
from vts.ft_fields_by_latestflight
WITH DATA;

ALTER TABLE vts.fields_by_latestflight
    OWNER TO root;
CREATE UNIQUE INDEX fblf_id
    ON vts.fields_by_latestflight USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
CREATE INDEX fblf_flight_id
    ON vts.fields_by_latestflight USING btree
        (flight_id)
    TABLESPACE pg_default;
CREATE INDEX fblf_flight_number
    ON vts.fields_by_latestflight USING btree
        (flight_number)
    TABLESPACE pg_default;
CREATE INDEX fblf_field_id
    ON vts.fields_by_latestflight USING btree
        (fieldid)
    TABLESPACE pg_default;

-- View: vts.fields_by_latestflight_ext

-- DROP VIEW vts.fields_by_latestflight_ext;

CREATE OR REPLACE VIEW vts.fields_by_latestflight_ext
AS
SELECT row_number() OVER ()                                                                                           AS "id",
       fl.geometry::geometry(Multipolygon, 3857),
       fl.fieldid,
       fl.fieldtoken,
       fl.fieldprecipitation,
       fl.fieldprecipitation48,
       fl.fieldprecipitation72,
       fl.fieldprecipitationtotal,
       fl.fieldplantedseeds,
       fl.fieldarea,
       fl.fieldthumbnailpath,
       fl.fieldisalert,
       fl.flight_date,
       fl.flight_id,
       fl.flight_thermal_variance,
       fl.flight_thermal_mean,
       fl.flight_thermal_max,
       fl.flight_thermal_min,
       fl.flight_code,
       fl.flight_scores,
       fl.crop_types,
       fl.flight_number,
       fl.features,
       case
           when (fl.features ->> 'field_harvested')::boolean
               then case when flight_number < 6 then 'Not Planted' else 'Harvested' end
           when (fl.features ->> 'field_emerged')::boolean then 'Emerged'
           when (fl.features ->> 'field_canopy_closed')::boolean then 'Canopy Closed'
           when (fl.features ->> 'field_planted')::boolean then 'Planted'
           when (fl.features ->> 'field_not_planted')::boolean then 'Not Planted'
           else 'Not Planted' end                                                                                     as field_state,
       (fl.features ->> 'field_crop_type')                                                                               field_crop_type,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'rowTracerPercent'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_percent,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'rowTracer'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_score,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'standCount'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_standCount,
       round((((fl.flight_scores -> 'ROW_TRACER'::text) ->> 'rank'::text)::real)::numeric,
             2)::double precision                                                                                     AS rowTracer_rank,
       round((((fl.flight_scores -> 'HEAT_SEEKER'::text) ->> 'temp_mean'::text)::real)::numeric,
             2)::double precision                                                                                     AS heatSeeker_temp_mean,
       round((((fl.flight_scores -> 'HEAT_SEEKER'::text) ->> 'temp_variance'::text)::real)::numeric,
             2)::double precision                                                                                     AS heatSeeker_temp_variance,
       round((((fl.flight_scores -> 'WEED_WATCH'::text) ->> 'area'::text)::real)::numeric,
             2)::double precision                                                                                     AS weedWatch_area,
       round((((fl.flight_scores -> 'WEED_WATCH'::text) ->> 'pct'::text)::real)::numeric,
             2)::double precision                                                                                     AS weedWatch_percent,
       round((((fl.flight_scores -> 'WEED_WATCH'::text) ->> 'rank'::text)::real)::numeric,
             2)::double precision                                                                                     AS weedWatch_rank,
       round((((fl.flight_scores -> 'TREND_ZONE'::text) ->> 'area'::text)::real)::numeric,
             2)::double precision                                                                                     AS trendzone_area,
       round((((fl.flight_scores -> 'TREND_ZONE'::text) ->> 'pct'::text)::real)::numeric,
             2)::double precision                                                                                     AS trendzone_percent,
       round((((fl.flight_scores -> 'YIELD_RISK'::text) ->> 'area'::text)::real)::numeric,
             2)::double precision                                                                                     AS yieldrisk_area,
       round((((fl.flight_scores -> 'YIELD_RISK'::text) ->> 'pct'::text)::real)::numeric,
             2)::double precision                                                                                     AS yieldrisk_percent
FROM vts.fields_by_latestflight fl;

ALTER TABLE vts.fields_by_latestflight_ext
    OWNER TO root;

-- refresh materialized view vts.fields_by_latestflight_ext

drop table if exists vts.test_lf;
select id,
       geometry::geometry(Multipolygon, 3857),
       fieldid,
       fieldtoken,
       fieldprecipitation,
       fieldprecipitation48,
       fieldprecipitation72,
       fieldprecipitationtotal,
       fieldplantedseeds,
       fieldarea,
       fieldthumbnailpath,
       fieldisalert,
       flight_date,
       flight_id,
       flight_thermal_variance,
       flight_thermal_mean,
       flight_thermal_max,
       flight_thermal_min,
       flight_code,
       flight_scores,
       crop_types,
       flight_number,
       features,
       field_state,
       field_crop_type,
       rowtracer_percent,
       rowtracer_score,
       rowtracer_standcount,
       rowtracer_rank,
       heatseeker_temp_mean,
       heatseeker_temp_variance,
       weedwatch_area,
       weedwatch_percent,
       weedwatch_rank,
       trendzone_area,
       trendzone_percent,
       yieldrisk_area,
       yieldrisk_percent
into vts.test_lf
from vts.fields_by_latestflight_ext f

ALTER TABLE vts.test_lf
    ADD CONSTRAINT test_lf_id_contraint UNIQUE (id);
CREATE INDEX test_lf_flight_id
    ON vts.test_lf USING btree
        (flight_id)
    TABLESPACE pg_default;
CREATE INDEX test_lf_flight_number
    ON vts.test_lf USING btree
        (flight_number)
    TABLESPACE pg_default;
CREATE INDEX test_lf_field_id
    ON vts.test_lf USING btree
        (fieldid)
    TABLESPACE pg_default;
CREATE INDEX test_lf_flight_number_field
    ON vts.test_lf USING btree
        (flight_number, fieldid)
    TABLESPACE pg_default;
