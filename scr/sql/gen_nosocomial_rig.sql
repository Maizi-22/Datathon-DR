-- This query pivots lab values taken in the first 24 hours of a patient's stay

-- Have already confirmed that the unit of measurement is always the same: null or the correct unit

DROP MATERIALIZED VIEW IF EXISTS nosocomial_check CASCADE;
CREATE materialized VIEW nosocomial_check AS

with raw_data as (
SELECT
  pvt.subject_id, pvt.hadm_id 
  , min(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_min
  , max(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_max
  , min(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_min
  , max(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_max
  , min(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END) as SODIUM_min
  , max(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end) as SODIUM_max
FROM
( -- begin query that extracts the data
  SELECT ie.subject_id, ie.hadm_id 
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
  , CASE
        WHEN itemid = 50862 THEN 'ALBUMIN'
        WHEN itemid = 50811 THEN 'HEMOGLOBIN'
        WHEN itemid = 51222 THEN 'HEMOGLOBIN'
        WHEN itemid = 50824 THEN 'SODIUM'
        WHEN itemid = 50983 THEN 'SODIUM'
      ELSE null
    END AS label
  , -- add in some sanity checks on the values
  -- the where clause below requires all valuenum to be > 0, so these are only upper limit checks
    CASE
      WHEN itemid = 50862 and valuenum <    0 THEN -1 -- g/dL 'ALBUMIN'              -- by CAOY remove the upper limits
      WHEN itemid = 50811 and valuenum <    0 THEN -1 -- g/dL 'HEMOGLOBIN'           -- by CAOY remove the upper limits
      WHEN itemid = 51222 and valuenum <    0 THEN -1 -- g/dL 'HEMOGLOBIN'           -- by CAOY remove the upper limits
      WHEN itemid = 50824 and valuenum <    0 THEN -1 -- mEq/L == mmol/L 'SODIUM'    -- by CAOY remove the upper limits
      WHEN itemid = 50983 and valuenum <    0 THEN -1 -- mEq/L == mmol/L 'SODIUM'    -- by CAOY remove the upper limits
    ELSE le.valuenum
    END AS valuenum

  FROM tab_zy ie                                                                       -- by CAOY use the cohort.csv of 20171216, should change the table name first !!

  LEFT JOIN labevents le
    ON le.subject_id = ie.subject_id AND le.hadm_id = ie.hadm_id
    AND le.charttime BETWEEN ie.admittime AND ie.rrt_charttime
    AND le.ITEMID in
    (
      -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
      50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
      51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
      50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
      50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
      50824 -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
    )
    AND valuenum IS NOT null AND valuenum > 0 -- lab values cannot be 0 and cannot be negative
) pvt
GROUP BY pvt.subject_id, pvt.hadm_id
ORDER BY pvt.subject_id, pvt.hadm_id)

select subject_id, hadm_id, ALBUMIN_min, HEMOGLOBIN_min, SODIUM_min,
-- judge Nosocomial_Hypoproteinemia by albumin_min < 3.5 g/ml, note the unit
case when ALBUMIN_min ISNULL then null when ALBUMIN_min < 3.5 then 1 else 0 end as Nosocomial_Hypoproteinemia, 
-- judge Nosocomial_Anemia by hemoglobin_min < 9.0 g/ml, note the unit
case when HEMOGLOBIN_min ISNULL then null when HEMOGLOBIN_min < 9.0 then 1 else 0 end as Nosocomial_Anemia, 
-- judge Nosocomial_Hyponatremia by sodium_min < 135 mM
case when SODIUM_min ISNULL then null when SODIUM_min < 135 then 1 else 0 end as Nosocomial_Hyponatremia
from raw_data;

