-- Quickly take a look at the data
SELECT *
FROM road_safety_events
LIMIT 10;

-- Check if there are any null values
SELECT *
FROM road_safety_events
WHERE occurrence_date IS NULL
  OR occurrence_detail IS NULL
  OR municipality IS NULL
  OR road_safety_occurrence_type IS NULL;
-- no null values

SELECT DISTINCT road_safety_occurrence_type
FROM road_safety_events;
-- 9 different types

SELECT DISTINCT location_code
FROM road_safety_events;
-- 9 different locations

SELECT DISTINCT municipality
FROM road_safety_events;
-- 9 different municipalities

SELECT DISTINCT involve_drug_or_alcohol
FROM road_safety_events;
-- 4 categories: alcohol, drugs, alcohol and drugs, neither

-- Number of accidents by region
SELECT
  municipality,
  COUNT(unique_identifier) AS num_accidents
FROM road_safety_events
GROUP BY municipality
ORDER BY num_accidents DESC;
-- Vaughan ranked first, Markham second, Richmond Hill third

-- Number of accidents by location
SELECT
  location_code,
  COUNT(unique_identifier) AS num_accidents,
  ROUND(COUNT(unique_identifier) * 100 / SUM(COUNT(unique_identifier)) OVER (), 2) AS percentage
FROM road_safety_events
GROUP BY location_code
ORDER BY num_accidents DESC;
-- Outdoor makes up 98%, residence around 1%, business around 1%

-- Number of accidents by type
SELECT
  road_safety_occurrence_type,
  COUNT(unique_identifier) AS num_accidents,
  ROUND(COUNT(unique_identifier) * 100 / SUM(COUNT(unique_identifier)) OVER (), 2) AS percentage
FROM road_safety_events
GROUP BY road_safety_occurrence_type
ORDER BY num_accidents DESC;
-- Collisions make up 90%, careless driving second, driving disqualified third

-- Number of accidents with alcohol and/or drug involved
SELECT
  involve_drug_or_alcohol,
  COUNT(unique_identifier) AS num_accidents,
  ROUND(COUNT(unique_identifier) * 100 / SUM(COUNT(unique_identifier)) OVER (), 2) AS percentage
FROM road_safety_events
GROUP BY involve_drug_or_alcohol
ORDER BY num_accidents DESC;
-- 94% involved no alcohol, 5% only involved alcohol, drugs only 0.4%, alc and drugs 0.22%
