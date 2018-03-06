SET SEARCH_PATH TO mimiciii;

-- This query extract vital signs : 
--   1 ) Heart rate : 211, 220045
--   2 ) SBP : 6, 51, 455, 6701, 220179, 220050
--   3 ) DBP : 8364, 8368, 8440, 8441, 8555, 220180, 220051
--   4 ) SPO2 : 646, 5820, 8554, 226253
--   5 ) Temperature : 223762, 676, 677 as celsius
--                     223761, 678, 679 as fahrenheit   (convert: (value-32) *5/9 )
--   6 ) CVP : 220074

-- We consider four timestamp:
--    1 ) First Time after Admit
--    2 ) First Time after DR
--    3 ) Last Time Before Dialysis
--    4 ) Last Time Before Discharge

WITH dr AS (
  SELECT dr.subject_id
         , dr.hadm_id
         , dr.admittime
         , dr.diuretic_resis_date
         , dr.rrt
         , dr.rrt_charttime
         , dr.get_better_date
         , dr.dischtime
    FROM diuretic_resis dr
)
-- Heart rate
-- First time after admit
, hr1 AS (
SELECT hr.subject_id
       , hr.hadm_id
       , hr.admittime
       , hr.charttime
       , value::NUMERIC AS hr_adm
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS hr_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (211, 220045)
                AND ce.value is not null
                AND ce.value::numeric <> 0
          WHERE ce.charttime >= admittime ) hr
 WHERE hr_seq = 1
)
-- First time after diuretic resist
, hr2 AS (
SELECT hr.subject_id
       , hr.hadm_id
       , hr.diuretic_resis_date
       , hr.charttime
       , value::numeric AS hr_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS hr_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (211, 220045)  
                AND ce.value::numeric is not null
                AND ce.value::numeric <> 0
          WHERE ce.charttime >= diuretic_resis_date ) hr
 WHERE hr_seq = 1
)

-- Last time before rrt(rrt patients)/urine increase
, hr3 AS (
SELECT hr.subject_id
       , hr.hadm_id
       , hr.diuretic_resis_date
       , hr.charttime
       , value::numeric AS hr_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS hr_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (211, 220045)  
                AND ce.value::numeric is not null
                AND ce.value::numeric <> 0
          WHERE (rrt = 1 AND ce.charttime >= diuretic_resis_date)) hr
 WHERE hr_seq = 1
 UNION ALL
SELECT hr.subject_id
       , hr.hadm_id
       , hr.diuretic_resis_date
       , hr.charttime
       , value::numeric AS hr_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS hr_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (211, 220045)  
                AND ce.value::numeric is not null
                AND ce.value::numeric <> 0
          WHERE (rrt IS NULL AND ce.charttime >= get_better_date)) hr
 WHERE hr_seq = 1
)

-- Last time before discharge
, hr4 AS (
SELECT hr.subject_id
       , hr.hadm_id
       , hr.dischtime
       , hr.charttime
       , value::numeric AS hr_dis
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime DESC) AS hr_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (211, 220045)  
                AND ce.value::numeric is not null
                AND ce.value::numeric <> 0
          WHERE ce.charttime <= dischtime ) hr
 WHERE hr_seq = 1
)
-- SBP on admission
, sbp1 AS (
SELECT sbp.subject_id
       , sbp.hadm_id
       , sbp.admittime
       , sbp.charttime
       , value::NUMERIC AS sbp_adm
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS sbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (6, 51, 455, 6701, 220179, 220050)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime >= admittime ) sbp
 WHERE sbp_seq = 1
)
-- SBP -- first time after DR 
, sbp2 AS (
SELECT sbp.subject_id
       , sbp.hadm_id
       , diuretic_resis_date
       , sbp.charttime
       , value::NUMERIC AS sbp_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS sbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (6, 51, 455, 6701, 220179, 220050)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime >= diuretic_resis_date ) sbp
 WHERE sbp_seq = 1
)
-- SBP -- Last time before rrt(rrt patients)/ increase urine
, sbp3 AS (
SELECT sbp.subject_id
       , sbp.hadm_id
       , diuretic_resis_date
       , sbp.charttime
       , value::NUMERIC AS sbp_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS sbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (6, 51, 455, 6701, 220179, 220050)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE rrt = 1 AND ce.charttime >= diuretic_resis_date ) sbp
 WHERE sbp_seq = 1
 UNION ALL
 SELECT sbp.subject_id
       , sbp.hadm_id
       , diuretic_resis_date
       , sbp.charttime
       , value::NUMERIC AS sbp_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS sbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (6, 51, 455, 6701, 220179, 220050)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE rrt IS NULL AND ce.charttime >= get_better_date ) sbp
 WHERE sbp_seq = 1
)
-- SBP before discharge
, sbp4 AS (
  SELECT sbp.subject_id
       , sbp.hadm_id
       , sbp.dischtime
       , sbp.charttime
       , value::NUMERIC AS sbp_dis
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime DESC) AS sbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (6, 51, 455, 6701, 220179, 220050)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime <= dischtime ) sbp
 WHERE sbp_seq = 1
)
-- DBP on admission
, dbp1 AS (
SELECT dbp.subject_id
       , dbp.hadm_id
       , dbp.admittime
       , dbp.charttime
       , value::NUMERIC AS dbp_adm
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS dbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (8364, 8368, 8440, 8441, 8555, 220180, 220051)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime >= admittime ) dbp
 WHERE dbp_seq = 1
)
-- DBP first time after DR
, dbp2 AS (
SELECT dbp.subject_id
       , dbp.hadm_id
       , diuretic_resis_date
       , dbp.charttime
       , value::NUMERIC AS dbp_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS dbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (8364, 8368, 8440, 8441, 8555, 220180, 220051)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime >= diuretic_resis_date ) dbp
 WHERE dbp_seq = 1
)
-- Last time before rrt(rrt patients)/ increase urine
, dbp3 AS (
SELECT dbp.subject_id
       , dbp.hadm_id
       , diuretic_resis_date
       , dbp.charttime
       , value::NUMERIC AS dbp_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS dbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (8364, 8368, 8440, 8441, 8555, 220180, 220051)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE rrt = 1 AND ce.charttime >= diuretic_resis_date ) dbp
 WHERE dbp_seq = 1
 UNION ALL 
 SELECT dbp.subject_id
       , dbp.hadm_id
       , diuretic_resis_date
       , dbp.charttime
       , value::NUMERIC AS dbp_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS dbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (8364, 8368, 8440, 8441, 8555, 220180, 220051)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE rrt IS NULL AND ce.charttime >= get_better_date ) dbp
 WHERE dbp_seq = 1
)
-- DBP on discharge
, dbp4 AS (
  SELECT dbp.subject_id
       , dbp.hadm_id
       , dbp.dischtime
       , dbp.charttime
       , value::NUMERIC AS dbp_dis
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime DESC) AS dbp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (8364, 8368, 8440, 8441, 8555, 220180, 220051)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime <= dischtime ) dbp
 WHERE dbp_seq = 1
)
-- SPO2
, spo21 AS (
SELECT spo2.subject_id
       , spo2.hadm_id
       , spo2.admittime
       , spo2.charttime
       , value::NUMERIC AS spo2_adm
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS spo2_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (646, 5820, 8554, 226253)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime >= admittime ) spo2
 WHERE spo2_seq = 1
)
-- SPO2 --First time after DR
, spo22 AS (
SELECT spo2.subject_id
       , spo2.hadm_id
       , spo2.diuretic_resis_date
       , spo2.charttime
       , value::NUMERIC AS spo2_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS spo2_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (646, 5820, 8554, 226253)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime >= diuretic_resis_date ) spo2
 WHERE spo2_seq = 1
)
-- SPO23 --First time after DR
, spo23 AS (
SELECT spo2.subject_id
       , spo2.hadm_id
       , spo2.diuretic_resis_date
       , spo2.charttime
       , value::NUMERIC AS spo2_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS spo2_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (646, 5820, 8554, 226253)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE rrt = 1 AND ce.charttime >= diuretic_resis_date ) spo2
 WHERE spo2_seq = 1
 UNION ALL
 SELECT spo2.subject_id
       , spo2.hadm_id
       , spo2.diuretic_resis_date
       , spo2.charttime
       , value::NUMERIC AS spo2_tem_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS spo2_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (646, 5820, 8554, 226253)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE rrt IS NULL AND ce.charttime >= get_better_date ) spo2
 WHERE spo2_seq = 1
)

-- SPO2 on discharge
, spo24 AS (
  SELECT spo2.subject_id
       , spo2.hadm_id
       , spo2.dischtime
       , spo2.charttime
       , value::NUMERIC AS spo2_dis
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime DESC) AS spo2_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (646, 5820, 8554, 226253)  
                AND ce.value is not null
                AND ce.value not like '0'
          WHERE ce.charttime <= dischtime ) spo2
 WHERE spo2_seq = 1
)
-- CVP on admission
, cvp1 AS (
SELECT cvp.subject_id
       , cvp.hadm_id
       , cvp.admittime
       , cvp.charttime
       , value::NUMERIC AS cvp_adm
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS cvp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (220074)  
                AND ce.value is not null
          WHERE ce.charttime >= admittime ) cvp
 WHERE cvp_seq = 1
)
-- CVP -- First after DR
, cvp2 AS (
SELECT cvp.subject_id
       , cvp.hadm_id
       , cvp.diuretic_resis_date
       , cvp.charttime
       , value::NUMERIC AS cvp_dr
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS cvp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (220074)  
                AND ce.value is not null
          WHERE ce.charttime >= diuretic_resis_date ) cvp
 WHERE cvp_seq = 1
)
-- CVP -- Last time before rrt(rrt patients)/ increase unine
, cvp3 AS (
SELECT cvp.subject_id
       , cvp.hadm_id
       , cvp.diuretic_resis_date
       , cvp.charttime
       , value::NUMERIC AS cvp_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS cvp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (220074)  
                AND ce.value is not null
          WHERE rrt = 1 AND ce.charttime >= diuretic_resis_date ) cvp
 WHERE cvp_seq = 1
 UNION ALL
 SELECT cvp.subject_id
       , cvp.hadm_id
       , cvp.diuretic_resis_date
       , cvp.charttime
       , value::NUMERIC AS cvp_tem_therapy
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS cvp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (220074)  
                AND ce.value is not null
          WHERE rrt IS NULL AND ce.charttime >= get_better_date ) cvp
 WHERE cvp_seq = 1
)

-- CVP on discharge
, cvp4 AS (
  SELECT cvp.subject_id
       , cvp.hadm_id
       , cvp.dischtime
       , cvp.charttime
       , value::NUMERIC AS cvp_dis
  FROM ( SELECT dr.*, ce.charttime, ce.value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime DESC) AS cvp_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (220074)  
                AND ce.value is not null
          WHERE ce.charttime <= dischtime ) cvp
 WHERE cvp_seq = 1
)
-- Temperature on admission
, tem1 AS (
SELECT tem.subject_id
       , tem.hadm_id
       , tem.admittime
       , tem.charttime
       , value AS tem_adm
  FROM ( SELECT dr.*, ce.charttime
                , CASE WHEN ce.itemid in (223762, 676, 677) THEN ce.value::NUMERIC
                       WHEN ce.itemid in (223761, 678, 679) THEN (ce.value::NUMERIC - 32) *5/9 END AS value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS tem_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (223762, 676, 677, 223761, 678, 679)  
                AND ce.value is not null
                AND ce.value != '0'
          WHERE ce.charttime >= admittime ) tem
 WHERE tem_seq = 1
)
-- Temperature -- First time after DR
, tem2 AS (
SELECT tem.subject_id
       , tem.hadm_id
       , tem.diuretic_resis_date
       , tem.charttime
       , value AS tem_dr
  FROM ( SELECT dr.*, ce.charttime
                , CASE WHEN ce.itemid in (223762, 676, 677) THEN ce.value::NUMERIC
                       WHEN ce.itemid in (223761, 678, 679) THEN (ce.value::NUMERIC - 32) *5/9 END AS value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS tem_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (223762, 676, 677, 223761, 678, 679)  
                AND ce.value is not null
                AND ce.value != '0'
          WHERE ce.charttime >= diuretic_resis_date ) tem
 WHERE tem_seq = 1
)
-- Temperature -- Last time before rrt(rrt patients)/ increase urine
, tem3 AS (
SELECT tem.subject_id
       , tem.hadm_id
       , tem.diuretic_resis_date
       , tem.charttime
       , value AS tem_therapy
  FROM ( SELECT dr.*, ce.charttime
                , CASE WHEN ce.itemid in (223762, 676, 677) THEN ce.value::NUMERIC
                       WHEN ce.itemid in (223761, 678, 679) THEN (ce.value::NUMERIC - 32) *5/9 END AS value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS tem_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (223762, 676, 677, 223761, 678, 679)  
                AND ce.value is not null
                AND ce.value != '0'
          WHERE rrt = 1 AND ce.charttime >= diuretic_resis_date ) tem
 WHERE tem_seq = 1
 UNION ALL
 SELECT tem.subject_id
       , tem.hadm_id
       , tem.diuretic_resis_date
       , tem.charttime
       , value AS tem_dr
  FROM ( SELECT dr.*, ce.charttime
                , CASE WHEN ce.itemid in (223762, 676, 677) THEN ce.value::NUMERIC
                       WHEN ce.itemid in (223761, 678, 679) THEN (ce.value::NUMERIC - 32) *5/9 END AS value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime) AS tem_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (223762, 676, 677, 223761, 678, 679)  
                AND ce.value is not null
                AND ce.value != '0'
          WHERE rrt IS NULL AND ce.charttime >= get_better_date ) tem
 WHERE tem_seq = 1
)
-- Temperature on discharge
, tem4 AS (
SELECT tem.subject_id
       , tem.hadm_id
       , tem.dischtime
       , tem.charttime
       , value AS tem_dis
  FROM ( SELECT dr.*, ce.charttime
                , CASE WHEN ce.itemid in (223762, 676, 677) THEN ce.value::NUMERIC
                       WHEN ce.itemid in (223761, 678, 679) THEN (ce.value::NUMERIC - 32) *5/9 END AS value
                , row_number() OVER (PARTITION BY ce.hadm_id ORDER BY ce.charttime DESC) AS tem_seq
           FROM dr
                LEFT JOIN chartevents ce
                ON ce.hadm_id = dr.hadm_id
                AND ce.itemid in (223762, 676, 677, 223761, 678, 679)  
                AND ce.value is not null
                AND ce.value != '0'
          WHERE ce.charttime <= dischtime ) tem
 WHERE tem_seq = 1
)

SELECT dr.subject_id
       , dr.hadm_id
       , hr1.hr_adm
       , hr2.hr_dr
       , hr3.hr_therapy
       , hr4.hr_dis
       , sbp1.sbp_adm
       , sbp2.sbp_dr
       , sbp3.sbp_therapy
       , sbp4.sbp_dis
       , dbp1.dbp_adm
       , dbp2.dbp_dr
       , dbp3.dbp_therapy
       , dbp4.dbp_dis
       , spo21.spo2_adm
       , spo22.spo2_dr
       , spo23.spo2_therapy
       , spo24.spo2_dis
       , cvp1.cvp_adm
       , cvp2.cvp_dr
       , cvp3.cvp_therapy
       , cvp4.cvp_dis
       , tem1.tem_adm
       , tem2.tem_dr
       , tem3.tem_therapy
       , tem4.tem_dis
  FROM dr
       LEFT JOIN hr1
       ON hr1.hadm_id = dr.hadm_id
       LEFT JOIN hr2
       ON hr2.hadm_id = dr.hadm_id
       LEFT JOIN hr3
       ON hr3.hadm_id = dr.hadm_id
       LEFT JOIN hr4
       ON hr4.hadm_id = dr.hadm_id
       LEFT JOIN sbp1
       ON sbp1.hadm_id = dr.hadm_id
       LEFT JOIN sbp2
       ON sbp2.hadm_id = dr.hadm_id
       LEFT JOIN sbp3
       ON sbp3.hadm_id = dr.hadm_id
       LEFT JOIN sbp4
       ON sbp4.hadm_id = dr.hadm_id
       LEFT JOIN dbp1
       ON dbp1.hadm_id = dr.hadm_id
       LEFT JOIN dbp2
       ON dbp2.hadm_id = dr.hadm_id
       LEFT JOIN dbp3
       ON dbp3.hadm_id = dr.hadm_id
       LEFT JOIN dbp4
       ON dbp4.hadm_id = dr.hadm_id
       LEFT JOIN spo21
       ON spo21.hadm_id = dr.hadm_id
       LEFT JOIN spo22
       ON spo22.hadm_id = dr.hadm_id
       LEFT JOIN spo23
       ON spo23.hadm_id = dr.hadm_id
       LEFT JOIN spo24
       ON spo24.hadm_id = dr.hadm_id
       LEFT JOIN cvp1
       ON cvp1.hadm_id = dr.hadm_id
       LEFT JOIN cvp2
       ON cvp2.hadm_id = dr.hadm_id
       LEFT JOIN cvp3
       ON cvp3.hadm_id = dr.hadm_id
       LEFT JOIN cvp4
       ON cvp4.hadm_id = dr.hadm_id
       LEFT JOIN tem1
       ON tem1.hadm_id = dr.hadm_id
       LEFT JOIN tem2
       ON tem2.hadm_id = dr.hadm_id
       LEFT JOIN tem3
       ON tem3.hadm_id = dr.hadm_id
       LEFT JOIN tem4
       ON tem4.hadm_id = dr.hadm_id
 ORDER BY dr.subject_id, dr.hadm_id

















