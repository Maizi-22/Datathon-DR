SET search_path TO mimiciii;

-- weight
WITH we AS (
  SELECT dr.subject_id
       , dr.hadm_id
       --, c.icustay_id
       , dr.diuretic_resis_date
       , dr.rrt_charttime as rrt_charttime
       , dr.get_better_date
       , c.charttime AS weight_charttime
       -- patient 1512(icustay_id = 276915) have a wrong weight record on admission
       , CASE WHEN c.icustay_id = '276915' AND c.valuenum = 139 THEN c.valuenum/2 ELSE c.valuenum END AS weight
       , row_number() OVER (PARTITION BY c.hadm_id ORDER BY c.charttime) AS hadm_seq
       , row_number() OVER (PARTITION BY c.hadm_id ORDER BY c.charttime DESC ) AS hadm_seq_desc
  FROM diuretic_resis dr
       LEFT JOIN icustays icus
       ON  icus.hadm_id = dr.hadm_id
       LEFT JOIN mimiciii.chartevents c
       ON c.icustay_id = icus.icustay_id
 WHERE ((c.valuenum IS NOT NULL)
   AND (c.itemid = ANY (ARRAY[762, 226512, 763, 224639]))
   AND (c.valuenum <> (0)::double precision)
   AND (c.error IS DISTINCT FROM 1))
)
, we2 AS (
  SELECT subject_id
       , hadm_id
       , weight_adm
       , weight_dis
       , weight_after_dr
       , first_weight_time
       , last_weight_time
  FROM ( SELECT we.subject_id
                , we.hadm_id
                , weight_adm
                , weight_dis
                , weight AS weight_after_dr
                , first_weight_time
                , last_weight_time
                , row_number()OVER (PARTITION BY we.hadm_id ORDER BY we.weight_charttime) AS dr_seq
           FROM we
                -- weight on admission
                LEFT JOIN (SELECT subject_id, hadm_id, weight_charttime as first_weight_time, weight AS weight_adm FROM we WHERE hadm_seq = 1) wef
                ON  wef.hadm_id = we.hadm_id
                -- weight on discharge
                LEFT JOIN (SELECT subject_id, hadm_id, weight_charttime as last_weight_time, weight AS weight_dis FROM we WHERE hadm_seq_desc = 1) wed
                ON  wed.hadm_id = we.hadm_id
          WHERE we.weight_charttime >= we.diuretic_resis_date ) weall
 WHERE weall.dr_seq = 1
)
-- Weight before dialysis
, we_bd AS(
  SELECT wd.*
    FROM (SELECT *
                 , row_number()OVER (PARTITION BY we.hadm_id ORDER BY we.weight_charttime DESC ) AS weight_d_seq
            FROM we
            WHERE we.rrt_charttime IS NOT NULL AND we.weight_charttime <= we.rrt_charttime ) wd
   WHERE wd.weight_d_seq = 1
  UNION ALL
  SELECT wd.*
    FROM (SELECT *
                 , row_number()OVER (PARTITION BY we.hadm_id ORDER BY we.weight_charttime DESC ) AS weight_d_seq
            FROM we
           WHERE we.rrt_charttime IS NULL AND we.weight_charttime <= we.get_better_date ) wd
   WHERE wd.weight_d_seq = 1 
)
-- height
, height AS (
  SELECT dr.subject_id
       , dr.hadm_id
       , percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY hd.height) AS height
  FROM diuretic_resis dr
       LEFT JOIN icustays icus
       ON  icus.hadm_id = dr.hadm_id
       LEFT JOIN heightallday hd
       ON hd.icustay_id = icus.icustay_id
 GROUP BY dr.subject_id
          , dr.hadm_id
)

SELECT dr.*
       , height.height
       , we2.weight_adm
       , we2.weight_after_dr
       , we_bd.weight AS weight_before_dialysis
       , we2.weight_dis
       , weight_adm/((height/100)^2) AS BMI
       , we_bd.weight - weight_adm AS weight_gain_before_rrt
       --, first_weight_time
       --, last_weight_time
       --, we_bd.weight_charttime AS weight_before_dialysis_time
       , CASE WHEN we_bd.weight_charttime <> first_weight_time THEN (we_bd.weight - weight_adm)/(date_part('epoch'::text, weight_charttime - first_weight_time )::NUMERIC/60/60/24) END AS weight_gain_before_rrt_perday
  FROM diuretic_resis dr
       LEFT JOIN height ON height.hadm_id = dr.hadm_id
       LEFT JOIN we2
       ON we2.hadm_id = height.hadm_id
       LEFT JOIN we_bd
       ON we_bd.hadm_id = height.hadm_id
