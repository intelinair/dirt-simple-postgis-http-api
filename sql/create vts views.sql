-- View: public.fields_by_flight

-- DROP VIEW public.fields_by_flight;

CREATE OR REPLACE VIEW public.fields_by_flight
AS
WITH a AS (
    WITH all_flights AS (
        SELECT generate_series(1, 13) AS flight_number
    )
       , latest_flight AS (
        SELECT DISTINCT ON (flight.field_id, af_1.flight_number) flight.field_id,
                                                                 flight.id,
                                                                 af_1.flight_number,
                                                                 (SELECT count(fc.id) AS count
                                                                  FROM flight fc
                                                                  WHERE fc.field_id = flight.field_id
                                                                    AND fc.status::text = 'COMPLETED'::text
                                                                    AND fc.released
                                                                    AND fc.season_id = 5) AS flight_count
        FROM flight
                 join all_flights af_1 on af_1.flight_number = flight.flight_number
        WHERE flight.season_id = 5
          AND flight.status::text = 'COMPLETED'::text
          AND flight.released
          AND flight.provider::text = 'Aeroptic'::text
        ORDER BY flight.field_id, af_1.flight_number, flight.date DESC
    )
       , all_fields_flights as (
        select f.id field_id, af_2.flight_number
        FROM all_flights af_2,
             field f
                 JOIN farm ff ON ff.id = f.farm_id
                 JOIN division d ON d.id = ff.division_id
                 JOIN company c ON c.id = d.company_id
                 join grid g2 on f.grid_id = g2.id -- must have a grid
        WHERE not f.deleted
          and not d.deleted
          and not ff.deleted
          and not c.deleted
          and c.enabled
          and f.is_monitored
    )
       , crop_type_list AS (
        SELECT fct.id                                                                 AS field_id,
               string_agg(DISTINCT ct.name::text, ','::text ORDER BY (ct.name::text)) AS crop_type_name
        FROM field fct
                 JOIN crop_type ct ON ct.key_name::text = ANY (fct.crop_type_keys::text[])
        GROUP BY fct.id
    )
    SELECT f.id                                                                                                    AS fieldid,
           f.precipitation                                                                                         AS fieldprecipitation,
           f.precipitation72                                                                                       AS fieldprecipitation72,
           f.planted_seeds                                                                                         AS fieldplantedseeds,
           f.grid_id                                                                                               AS fieldgridid,
           f.center_longitude                                                                                      AS fieldcenterlongitude,
           f.name                                                                                                  AS fieldname,
           f.area                                                                                                  AS fieldarea,
           f.precipitation48                                                                                       AS fieldprecipitation48,
           f.farm_id                                                                                               AS fieldfarmid,
           f.geo_data_id                                                                                           AS fieldgeodataid,
           f.last_boundary_sync                                                                                    AS fieldlastboundarysync,
           f.last_updated                                                                                          AS fieldlastupdated,
           f.is_monitored                                                                                          AS fieldismonitored,
           f.deleted                                                                                               AS fieldisdeleted,
           f.precipitation_total                                                                                   AS fieldprecipitationtotal,
           f.token                                                                                                 AS fieldtoken,
           f.center_latitude                                                                                       AS fieldcenterlatitude,
           f.planting_date                                                                                         AS fieldplantingdate,
           date_part('week'::text, f.planting_date)                                                                AS fieldplantingweek,
           f.thumbnail_path                                                                                        AS fieldthumbnailpath,
           f.growing_degree_days                                                                                   AS fieldgrowingdegreedays,
           f.is_alert                                                                                              AS fieldisalert,
           f.is_favorite                                                                                           AS fieldisfavorite,
           f.description                                                                                           AS fielddescription,
           fl.id                                                                                                   AS latestflightid,
           fl.date_created                                                                                         AS latestflightdatecreated,
           fl.thermal_variance                                                                                     AS latestflightthermalvariance,
           fl.season_id                                                                                            AS latestflightseasonid,
           fl.north_east_latitude                                                                                  AS latestflightnortheastlatitude,
           fl.center_longitude                                                                                     AS latestflightcenterlongitude,
           fl.thermal_mean                                                                                         AS latestflightthermalmean,
           fl.date                                                                                                 AS latestflightdate,
           fl.code                                                                                                 AS latestflightcode,
           fl.provider                                                                                             AS latestflightprovider,
           fl.thermal_max                                                                                          AS latestflightthermalmax,
           fl.status                                                                                               AS latestflightstatus,
           fl.field_id                                                                                             AS latestflightfieldid,
           fl.thermal_min                                                                                          AS latestflightthermalmin,
           fl.scouting_report_id                                                                                   AS latestflightscoutingreportid,
           lf.flight_count,
           fl.thumbnail_path                                                                                       AS latestflighttumbnailpath,
           fl.health                                                                                               AS latestflighthealth,
           fl.release_date                                                                                         AS latestflightreleasedate,
           fl.released                                                                                             AS latestflightreleased,
           fl.weediness                                                                                            AS latestflightweediness,
           fl.weediness_mode                                                                                       AS latestflightweedinessmode,
           fl.flight_number                                                                                        AS latestflightnumber,
           fl.previous_code                                                                                        AS latestflightpreviouscode,
           fl.coverage                                                                                             AS latestflightcoverage,
           fl.flightscores::text                                                                                   AS latestflightflightscores,
           round((((fl.flightscores -> 'WEED_WATCH'::text) ->> 'area'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightweedwatcharea,
           round((((fl.flightscores -> 'TREND_ZONE'::text) ->> 'area'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflighttrendzonearea,
           round((((fl.flightscores -> 'YIELD_RISK'::text) ->> 'area'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightyieldriskarea,
           round((((fl.flightscores -> 'ROW_TRACER'::text) ->> 'rowTracerPercent'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightrowtracerpercent,
           round((((fl.flightscores -> 'ROW_TRACER'::text) ->> 'rowTracer'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightrowtracer,
           round((((fl.flightscores -> 'ROW_TRACER'::text) ->> 'standCount'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightstandcount,
           round((((fl.flightscores -> 'YIELD_RISK'::text) ->> 'pct'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightyieldriskpercent,
           round((((fl.flightscores -> 'TREND_ZONE'::text) ->> 'pct'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflighttrendzonepercent,
           CASE
               WHEN lf.flight_count = 0 THEN 0
               ELSE 1
               END                                                                                                 AS has_flight,
           ff.id                                                                                                   AS farmid,
           ff.deleted                                                                                              AS farmdeleted,
           ff.division_id                                                                                          AS farmdivisionid,
           ff.name                                                                                                 AS farmname,
           d.id                                                                                                    AS divisionid,
           c.id                                                                                                    AS companyid,
           d.deleted                                                                                               AS divisiondeleted,
           d.company_id                                                                                            AS divisioncompanyid,
           c.name                                                                                                  AS companyname,
           c.is_satellite_enabled                                                                                  AS issatelliteenabled,
           d.name                                                                                                  AS divisionname,
           ca.id                                                                                                   AS companyaliasid,
           ca.company_hierarchy_id                                                                                 AS companyaliascompanyhierarchyid,
           ch.id                                                                                                   AS companyhierarchyid,
           ch.name                                                                                                 AS companyhierarchyname,
           gd.id                                                                                                   AS geodataid,
           gd.geometry                                                                                             AS fieldgeometry,
           g.id                                                                                                    AS gridid,
           g.grid_name                                                                                             AS gridname,
           g.season_id                                                                                             AS gridseasonid,
           g.schedule_id                                                                                           AS gridscheduleid,
           ctl.crop_type_name                                                                                      AS fieldcroptypes,
           af.flight_number
    FROM all_fields_flights af
             join field f on f.id = af.field_id
             JOIN farm ff ON ff.id = f.farm_id
             JOIN division d ON d.id = ff.division_id
             JOIN company c ON c.id = d.company_id
             JOIN company_alias ca ON ca.id = c.alias_id
             JOIN company_hierarchy ch ON ch.id = ca.company_hierarchy_id
             LEFT JOIN latest_flight lf ON lf.field_id = af.field_id and lf.flight_number = af.flight_number
             LEFT JOIN flight fl ON fl.id = lf.id
             LEFT JOIN geo_data gd ON gd.id = f.geo_data_id
             LEFT JOIN crop_type_list ctl ON ctl.field_id = f.id
             LEFT JOIN grid g ON g.id = f.grid_id
)
SELECT a.fieldid,
       a.fieldtoken,
       a.fieldprecipitation,
       a.fieldprecipitation48,
       a.fieldprecipitation72,
       a.fieldprecipitationtotal,
       a.fieldgrowingdegreedays                      AS fieldplantedseeds,
       round(a.fieldarea, 2)                         AS fieldarea,
       a.fieldthumbnailpath,
       a.fieldisalert,
       a.latestflightdate                            AS flight_date,
       a.latestflightid                              AS flight_id,
       a.latestflightthermalvariance                 AS flight_thermal_variance,
       a.latestflightthermalmean                     AS flight_thermal_mean,
       a.latestflightthermalmax                      AS flight_thermal_max,
       a.latestflightthermalmin                      AS flight_thermal_min,
       a.latestflightcode                            AS flight_code,
       a.latestflightflightscores::jsonb             AS flight_scores,
       a.fieldgeometry::geometry(Multipolygon, 4326) AS geometry,
       a.fieldcroptypes                              AS crop_types,
       a.flight_number,
       bf.features
FROM a
         LEFT JOIN base_feature bf ON bf.flight_id = a.latestflightid;

ALTER TABLE public.fields_by_flight
    OWNER TO pgadmin;

GRANT ALL ON TABLE public.fields_by_flight TO pgadmin;
GRANT SELECT ON TABLE public.fields_by_flight TO looker;

-- View: public.fields_by_latestflight

-- DROP VIEW public.fields_by_latestflight;

CREATE OR REPLACE VIEW public.fields_by_latestflight
AS
WITH a AS (
    WITH latest_flight AS (
        SELECT DISTINCT ON (flight.field_id) flight.field_id,
                                             flight.id,
                                             flight.flight_number,
                                             (SELECT count(fc.id) AS count
                                              FROM flight fc
                                              WHERE fc.field_id = flight.field_id
                                                AND fc.status::text = 'COMPLETED'::text
                                                AND fc.released
                                                AND fc.season_id = 5) AS flight_count
        FROM flight
        WHERE flight.season_id = 5
          AND flight.status::text = 'COMPLETED'::text
          AND flight.released
          AND flight.provider::text = 'Aeroptic'::text
        ORDER BY flight.field_id, flight.date DESC
    ),
         all_fields_flights AS (
             SELECT f_1.id AS field_id,
                    af_2.flight_number
             FROM field f_1
                      join latest_flight af_2 on af_2.field_id = f_1.id
                      JOIN farm ff_1 ON ff_1.id = f_1.farm_id
                      JOIN division d_1 ON d_1.id = ff_1.division_id
                      JOIN company c_1 ON c_1.id = d_1.company_id
                      JOIN grid g2 ON f_1.grid_id = g2.id
             WHERE NOT f_1.deleted
               AND NOT d_1.deleted
               AND NOT ff_1.deleted
               AND NOT c_1.deleted
               AND c_1.enabled
               AND f_1.is_monitored
         ),
         crop_type_list AS (
             SELECT fct.id                                                                 AS field_id,
                    string_agg(DISTINCT ct.name::text, ','::text ORDER BY (ct.name::text)) AS crop_type_name
             FROM field fct
                      JOIN crop_type ct ON ct.key_name::text = ANY (fct.crop_type_keys::text[])
             GROUP BY fct.id
         )
    SELECT f.id                                                                                                    AS fieldid,
           f.precipitation                                                                                         AS fieldprecipitation,
           f.precipitation72                                                                                       AS fieldprecipitation72,
           f.planted_seeds                                                                                         AS fieldplantedseeds,
           f.grid_id                                                                                               AS fieldgridid,
           f.center_longitude                                                                                      AS fieldcenterlongitude,
           f.name                                                                                                  AS fieldname,
           f.area                                                                                                  AS fieldarea,
           f.precipitation48                                                                                       AS fieldprecipitation48,
           f.farm_id                                                                                               AS fieldfarmid,
           f.geo_data_id                                                                                           AS fieldgeodataid,
           f.last_boundary_sync                                                                                    AS fieldlastboundarysync,
           f.last_updated                                                                                          AS fieldlastupdated,
           f.is_monitored                                                                                          AS fieldismonitored,
           f.deleted                                                                                               AS fieldisdeleted,
           f.precipitation_total                                                                                   AS fieldprecipitationtotal,
           f.token                                                                                                 AS fieldtoken,
           f.center_latitude                                                                                       AS fieldcenterlatitude,
           f.planting_date                                                                                         AS fieldplantingdate,
           date_part('week'::text, f.planting_date)                                                                AS fieldplantingweek,
           f.thumbnail_path                                                                                        AS fieldthumbnailpath,
           f.growing_degree_days                                                                                   AS fieldgrowingdegreedays,
           f.is_alert                                                                                              AS fieldisalert,
           f.is_favorite                                                                                           AS fieldisfavorite,
           f.description                                                                                           AS fielddescription,
           fl.id                                                                                                   AS latestflightid,
           fl.date_created                                                                                         AS latestflightdatecreated,
           fl.thermal_variance                                                                                     AS latestflightthermalvariance,
           fl.season_id                                                                                            AS latestflightseasonid,
           fl.north_east_latitude                                                                                  AS latestflightnortheastlatitude,
           fl.center_longitude                                                                                     AS latestflightcenterlongitude,
           fl.thermal_mean                                                                                         AS latestflightthermalmean,
           fl.date                                                                                                 AS latestflightdate,
           fl.code                                                                                                 AS latestflightcode,
           fl.provider                                                                                             AS latestflightprovider,
           fl.thermal_max                                                                                          AS latestflightthermalmax,
           fl.status                                                                                               AS latestflightstatus,
           fl.field_id                                                                                             AS latestflightfieldid,
           fl.thermal_min                                                                                          AS latestflightthermalmin,
           fl.scouting_report_id                                                                                   AS latestflightscoutingreportid,
           lf.flight_count,
           fl.thumbnail_path                                                                                       AS latestflighttumbnailpath,
           fl.health                                                                                               AS latestflighthealth,
           fl.release_date                                                                                         AS latestflightreleasedate,
           fl.released                                                                                             AS latestflightreleased,
           fl.weediness                                                                                            AS latestflightweediness,
           fl.weediness_mode                                                                                       AS latestflightweedinessmode,
           fl.flight_number                                                                                        AS latestflightnumber,
           fl.previous_code                                                                                        AS latestflightpreviouscode,
           fl.coverage                                                                                             AS latestflightcoverage,
           fl.flightscores::text                                                                                   AS latestflightflightscores,
           round((((fl.flightscores -> 'WEED_WATCH'::text) ->> 'area'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightweedwatcharea,
           round((((fl.flightscores -> 'TREND_ZONE'::text) ->> 'area'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflighttrendzonearea,
           round((((fl.flightscores -> 'YIELD_RISK'::text) ->> 'area'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightyieldriskarea,
           round((((fl.flightscores -> 'ROW_TRACER'::text) ->> 'rowTracerPercent'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightrowtracerpercent,
           round((((fl.flightscores -> 'ROW_TRACER'::text) ->> 'rowTracer'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightrowtracer,
           round((((fl.flightscores -> 'ROW_TRACER'::text) ->> 'standCount'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightstandcount,
           round((((fl.flightscores -> 'YIELD_RISK'::text) ->> 'pct'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflightyieldriskpercent,
           round((((fl.flightscores -> 'TREND_ZONE'::text) ->> 'pct'::text)::real)::numeric,
                 2)::double precision                                                                              AS latestflighttrendzonepercent,
           CASE
               WHEN lf.flight_count = 0 THEN 0
               ELSE 1
               END                                                                                                 AS has_flight,
           ff.id                                                                                                   AS farmid,
           ff.deleted                                                                                              AS farmdeleted,
           ff.division_id                                                                                          AS farmdivisionid,
           ff.name                                                                                                 AS farmname,
           d.id                                                                                                    AS divisionid,
           c.id                                                                                                    AS companyid,
           d.deleted                                                                                               AS divisiondeleted,
           d.company_id                                                                                            AS divisioncompanyid,
           c.name                                                                                                  AS companyname,
           c.is_satellite_enabled                                                                                  AS issatelliteenabled,
           d.name                                                                                                  AS divisionname,
           ca.id                                                                                                   AS companyaliasid,
           ca.company_hierarchy_id                                                                                 AS companyaliascompanyhierarchyid,
           ch.id                                                                                                   AS companyhierarchyid,
           ch.name                                                                                                 AS companyhierarchyname,
           gd.id                                                                                                   AS geodataid,
           gd.geometry                                                                                             AS fieldgeometry,
           g.id                                                                                                    AS gridid,
           g.grid_name                                                                                             AS gridname,
           g.season_id                                                                                             AS gridseasonid,
           g.schedule_id                                                                                           AS gridscheduleid,
           ctl.crop_type_name                                                                                      AS fieldcroptypes,
           af.flight_number
    FROM all_fields_flights af
             join field f on f.id = af.field_id
             JOIN farm ff ON ff.id = f.farm_id
             JOIN division d ON d.id = ff.division_id
             JOIN company c ON c.id = d.company_id
             JOIN company_alias ca ON ca.id = c.alias_id
             JOIN company_hierarchy ch ON ch.id = ca.company_hierarchy_id
             LEFT JOIN latest_flight lf ON lf.field_id = af.field_id and lf.flight_number = af.flight_number
             LEFT JOIN flight fl ON fl.id = lf.id
             LEFT JOIN geo_data gd ON gd.id = f.geo_data_id
             LEFT JOIN crop_type_list ctl ON ctl.field_id = f.id
             LEFT JOIN grid g ON g.id = f.grid_id
)
SELECT a.fieldid,
       a.fieldtoken,
       a.fieldprecipitation,
       a.fieldprecipitation48,
       a.fieldprecipitation72,
       a.fieldprecipitationtotal,
       a.fieldgrowingdegreedays                      AS fieldplantedseeds,
       round(a.fieldarea, 2)                         AS fieldarea,
       a.fieldthumbnailpath,
       a.fieldisalert,
       a.latestflightdate                            AS flight_date,
       a.latestflightid                              AS flight_id,
       a.latestflightthermalvariance                 AS flight_thermal_variance,
       a.latestflightthermalmean                     AS flight_thermal_mean,
       a.latestflightthermalmax                      AS flight_thermal_max,
       a.latestflightthermalmin                      AS flight_thermal_min,
       a.latestflightcode                            AS flight_code,
       a.latestflightflightscores::jsonb             AS flight_scores,
       a.fieldgeometry::geometry(Multipolygon, 4326) AS geometry,
       a.fieldcroptypes                              AS crop_types,
       a.flight_number,
       bf.features
FROM a
         LEFT JOIN base_feature bf ON bf.flight_id = a.latestflightid;

ALTER TABLE public.fields_by_latestflight
    OWNER TO pgadmin;

GRANT ALL ON TABLE public.fields_by_latestflight TO pgadmin;
GRANT SELECT ON TABLE public.fields_by_latestflight TO looker;

