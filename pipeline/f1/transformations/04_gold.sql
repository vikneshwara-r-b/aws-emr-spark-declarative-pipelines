-- Modelled (gold) zone: materialized views over the curated Auto CDC tables. the facts
-- are SCD1 (latest state only). dim_drivers, dim_teams and dim_circuits are SCD2 (full history, one row
-- per version), so the views below read their current versions only (__END_AT IS NULL); joining the
-- history rows would multiply every fact row by the number of versions of its driver/team/circuit.
-- Positions are integers; a retired driver still has a numeric classified position in Jolpica
-- (position_text = 'R').

CREATE TEMPORARY VIEW dim_drivers_current AS SELECT * FROM dim_drivers WHERE __END_AT IS NULL;
CREATE TEMPORARY VIEW dim_teams_current AS SELECT * FROM dim_teams WHERE __END_AT IS NULL;
CREATE TEMPORARY VIEW dim_circuits_current AS SELECT * FROM dim_circuits WHERE __END_AT IS NULL;

CREATE MATERIALIZED VIEW driver_performance_summary AS
SELECT r.season_year, d.driver_Id, concat(d.given_name, ' ', d.family_name) AS driver_name,
  t.team_id AS team_Id, t.name AS team_name,
  COUNT(DISTINCT r.race_Id) AS races_entered,
  COUNT(CASE WHEN r.final_position = 1 THEN d.driver_Id END) AS wins,
  COUNT(CASE WHEN r.final_position <= 3 THEN d.driver_Id END) AS podiums,
  SUM(r.points) AS total_points,
  AVG(r.final_position) AS avg_finish_position,
  MIN(r.final_position) AS best_finish_position,
  current_timestamp() AS refreshed_at
FROM fact_race_results r
INNER JOIN dim_drivers_current d ON r.driver_Id = d.driver_Id
INNER JOIN dim_teams_current t ON r.team_Id = t.team_id
GROUP BY r.season_year, d.driver_Id, d.given_name, d.family_name, t.team_id, t.name;

CREATE MATERIALIZED VIEW team_performance_summary AS
SELECT r.season_year, t.team_id AS team_Id, t.name AS team_name,
  COUNT(DISTINCT r.race_Id) AS races_entered,
  SUM(CASE WHEN r.final_position = 1 THEN 1 ELSE 0 END) AS wins,
  SUM(CASE WHEN r.final_position <= 3 THEN 1 ELSE 0 END) AS podiums,
  SUM(r.points) AS total_points,
  COUNT(DISTINCT r.driver_Id) AS drivers_used,
  current_timestamp() AS refreshed_at
FROM fact_race_results r
INNER JOIN dim_teams_current t ON r.team_Id = t.team_id
GROUP BY r.season_year, t.team_id, t.name;

-- track_type (from the second, independent circuit_track_type reference source -- see
-- 03_curated.py) passes through here, proving propagation all the way to gold. The fastest lap is
-- computed from the ingested races (Jolpica has no circuit lap record).
CREATE MATERIALIZED VIEW circuit_summary AS
SELECT c.circuit_Id, c.name AS circuit_name, c.locality, c.country, c.track_type, c.lat, c.lon,
  COUNT(DISTINCT r.race_Id) AS total_races_held,
  MIN(r.race_Date) AS first_race_date,
  MAX(r.race_Date) AS last_race_date,
  MIN(r.fastest_lap_time_ms) AS fastest_lap_time_ms,
  MIN_BY(r.driver_Id, r.fastest_lap_time_ms) AS fastest_lap_driver_id,
  current_timestamp() AS refreshed_at
FROM dim_circuits_current c
INNER JOIN fact_race_results r ON c.circuit_Id = r.circuit_Id
GROUP BY c.circuit_Id, c.name, c.locality, c.country, c.track_type, c.lat, c.lon;

-- Qualifying vs race: positions_gained > 0 means the driver finished ahead of where they qualified.
CREATE MATERIALIZED VIEW qualifying_vs_race_summary AS
SELECT r.season_year, d.driver_Id, concat(d.given_name, ' ', d.family_name) AS driver_name,
  COUNT(DISTINCT r.race_Id) AS races_entered,
  COUNT(CASE WHEN q.qual_position = 1 THEN 1 END) AS poles,
  AVG(q.qual_position) AS avg_qualifying_position,
  AVG(r.final_position) AS avg_finish_position,
  AVG(q.qual_position - r.final_position) AS avg_positions_gained,
  current_timestamp() AS refreshed_at
FROM fact_race_results r
INNER JOIN fact_qualifying q ON r.race_Id = q.race_Id AND r.driver_Id = q.driver_Id
INNER JOIN dim_drivers_current d ON r.driver_Id = d.driver_Id
GROUP BY r.season_year, d.driver_Id, d.given_name, d.family_name;

-- Pit stops per constructor. Stops longer than 60 s are excluded as red-flag outliers (the API
-- reports some as e.g. '40:55.302'); the raw duration stays available in fact_pit_stops.
CREATE MATERIALIZED VIEW pit_stop_summary AS
SELECT p.season_year, t.team_id AS team_Id, t.name AS team_name,
  COUNT(*) AS stops,
  COUNT(DISTINCT p.driver_Id) AS drivers,
  AVG(p.duration_ms) AS avg_duration_ms,
  MIN(p.duration_ms) AS best_duration_ms,
  current_timestamp() AS refreshed_at
FROM fact_pit_stops p
INNER JOIN fact_race_results r ON p.race_Id = r.race_Id AND p.driver_Id = r.driver_Id
INNER JOIN dim_teams_current t ON r.team_Id = t.team_id
WHERE p.duration_ms IS NOT NULL AND p.duration_ms <= 60000
GROUP BY p.season_year, t.team_id, t.name;
