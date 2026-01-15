-- Road Safety Events Data Analysis
-- This file contains SQL queries for analyzing the road_safety_events table

-- Total number of events
SELECT COUNT(*) AS total_events
FROM road_safety_events;

-- Date range of data
SELECT 
    MIN(occurrence_date) AS earliest_date,
    MAX(occurrence_date) AS latest_date,
    MAX(occurrence_date) - MIN(occurrence_date) AS date_range
FROM road_safety_events
WHERE occurrence_date IS NOT NULL;

-- Events by municipality with percentages
SELECT
    municipality,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM road_safety_events
WHERE municipality IS NOT NULL
GROUP BY municipality
ORDER BY event_count DESC;

-- Events by road safety occurrence type
SELECT
    road_safety_occurrence_type,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM road_safety_events
WHERE road_safety_occurrence_type IS NOT NULL
GROUP BY road_safety_occurrence_type
ORDER BY event_count DESC;

-- Events by location code
SELECT
    location_code,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM road_safety_events
WHERE location_code IS NOT NULL
GROUP BY location_code
ORDER BY event_count DESC;

-- Events involving drugs/alcohol
SELECT
    involve_drug_or_alcohol,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM road_safety_events
WHERE involve_drug_or_alcohol IS NOT NULL
GROUP BY involve_drug_or_alcohol
ORDER BY event_count DESC;

-- Events by date (daily aggregation)
SELECT
    DATE(occurrence_date) AS event_date,
    COUNT(*) AS event_count
FROM road_safety_events
WHERE occurrence_date IS NOT NULL
GROUP BY DATE(occurrence_date)
ORDER BY event_date DESC;

-- Events by hour of day
SELECT
    EXTRACT(HOUR FROM time_est) AS hour_of_day,
    COUNT(*) AS event_count
FROM road_safety_events
WHERE time_est IS NOT NULL
GROUP BY EXTRACT(HOUR FROM time_est)
ORDER BY hour_of_day;

-- Events by day of week (0=Sunday, 6=Saturday)
SELECT
    EXTRACT(DOW FROM occurrence_date) AS day_of_week,
    CASE EXTRACT(DOW FROM occurrence_date)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_name,
    COUNT(*) AS event_count
FROM road_safety_events
WHERE occurrence_date IS NOT NULL
GROUP BY EXTRACT(DOW FROM occurrence_date)
ORDER BY day_of_week;

-- Events by type and municipality (top combinations)
SELECT
    municipality,
    road_safety_occurrence_type,
    COUNT(*) AS event_count
FROM road_safety_events
WHERE municipality IS NOT NULL 
    AND road_safety_occurrence_type IS NOT NULL
GROUP BY municipality, road_safety_occurrence_type
ORDER BY event_count DESC
LIMIT 20;

-- Events by type and drug/alcohol involvement
SELECT
    road_safety_occurrence_type,
    involve_drug_or_alcohol,
    COUNT(*) AS event_count
FROM road_safety_events
WHERE road_safety_occurrence_type IS NOT NULL
    AND involve_drug_or_alcohol IS NOT NULL
GROUP BY road_safety_occurrence_type, involve_drug_or_alcohol
ORDER BY road_safety_occurrence_type, event_count DESC;

-- ============================================================================
-- DATA QUALITY CHECKS
-- ============================================================================

-- Check for missing values
SELECT
    COUNT(*) AS total_rows,
    COUNT(occurrence_date) AS rows_with_date,
    COUNT(road_safety_occurrence_type) AS rows_with_type,
    COUNT(municipality) AS rows_with_municipality,
    COUNT(x) AS rows_with_x,
    COUNT(y) AS rows_with_y,
    COUNT(time_est) AS rows_with_time
FROM road_safety_events;

-- Most recent events
SELECT
    unique_identifier,
    occurrence_date,
    municipality,
    road_safety_occurrence_type,
    location_code,
    involve_drug_or_alcohol
FROM road_safety_events
WHERE occurrence_date IS NOT NULL
ORDER BY occurrence_date DESC
LIMIT 100;
