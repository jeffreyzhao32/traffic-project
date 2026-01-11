ALTER TABLE road_safety_events
  RENAME COLUMN "Occurrence Date" TO occurrence_date;

ALTER TABLE road_safety_events
  RENAME COLUMN "Occurrence Detail" TO occurrence_detail;

ALTER TABLE road_safety_events
  RENAME COLUMN "UniqueIdentifier" TO unique_identifier;

ALTER TABLE road_safety_events
  RENAME COLUMN "Location Code" TO location_code;

ALTER TABLE road_safety_events
  RENAME COLUMN "Involve Drug or Alcohol" TO involve_drug_or_alcohol;

ALTER TABLE road_safety_events
  RENAME COLUMN "Occurrence Number" TO occurrence_number;

ALTER TABLE road_safety_events
  RENAME COLUMN "Road Safety Occurrence Type" TO road_safety_occurrence_type;

ALTER TABLE road_safety_events
  RENAME COLUMN "Municipality" TO municipality;

ALTER TABLE road_safety_events
  DROP COLUMN "OBJECTID";
