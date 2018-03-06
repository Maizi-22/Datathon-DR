SET SEARCH_PATH TO mimiciii;

-- This query calculate Total Fluid Overload Before Dialysis
-- For inputevents, there are several unit, of which we considered fluid are : cc, ml, tsp(one tea spoon, equals 5ml), ounces(equals 29.27ml)
-- For outputevents, only 'ml' are recorded.
-- All units are convert to ml
WITH input_rrt AS (
  SELECT ip.subject_id, ip.hadm_id, ip.adm_to_rrt_day
      , sum(ip.amount) AS total_input_to_rrt
  FROM(
    SELECT ic.subject_id, ic.hadm_id, ic.charttime, dr.rrt, dr.rrt_charttime, dr.diuretic_resis_date
           , CASE WHEN rrt = 1 THEN (date_part('day'::text, dr.rrt_charttime - dr.admittime ))
                  WHEN rrt ISNULL THEN (date_part('day'::text, dr.diuretic_resis_date - dr.admittime )) + 1 END AS adm_to_rrt_day
           , ic.amountuom
           , CASE WHEN ic.amountuom like 'tsp' THEN amount/5
                  ELSE ic.amount END AS amount
      FROM inputevents_cv ic
           LEFT JOIN diuretic_resis dr
           ON dr.hadm_id = ic.hadm_id
     WHERE ic.amountuom::text like ANY (ARRAY['cc', 'ml', 'tsp'])
       AND ic.hadm_id = dr.hadm_id
     UNION ALL
    SELECT im.subject_id, im.hadm_id, im.endtime AS charttime, dr.rrt, dr.rrt_charttime, dr.diuretic_resis_date
           , CASE WHEN rrt = 1 THEN (date_part('day'::text, dr.rrt_charttime - dr.admittime ))
                  WHEN rrt ISNULL THEN (date_part('day'::text, dr.diuretic_resis_date - dr.admittime )) + 1 END AS adm_to_rrt_day
           , im.amountuom
           , CASE WHEN im.amountuom like 'uL' THEN im.amount/1000
                  WHEN im.amountuom like 'ounces' THEN im.amount*29.27
                  ELSE im.amount END AS amount
      FROM inputevents_mv im
           LEFT JOIN diuretic_resis dr
           ON dr.hadm_id = im.hadm_id
     WHERE im.amountuom::text like ANY (ARRAY['ml', 'uL', 'ounces'])
       AND im.hadm_id = dr.hadm_id ) ip
   WHERE ((rrt = 1 AND charttime <= rrt_charttime) OR (rrt ISNULL AND charttime <= (diuretic_resis_date + '1 day'::interval day)))
   GROUP BY subject_id, hadm_id, ip.adm_to_rrt_day
)
, output_rrt AS(
SELECT op.hadm_id
      , sum(op.amount) AS total_output_to_rrt
  FROM ( select op.hadm_id, op.charttime, value as amount, dr.rrt, dr.rrt_charttime, dr.diuretic_resis_date
           from outputevents op
                left join diuretic_resis dr
                on op.hadm_id = dr.hadm_id
          where op.hadm_id = dr.hadm_id) op
 WHERE ((op.rrt = 1 AND op.charttime <= op.rrt_charttime) OR (op.rrt ISNULL AND op.charttime <= (op.diuretic_resis_date + '1 day'::interval day)))
   GROUP BY op.hadm_id
)
, input_dis AS (
  SELECT ip.hadm_id
      , sum(ip.amount) AS total_input_to_discharge
  FROM(
    SELECT ic.hadm_id, ic.charttime, dr.rrt, dr.dischtime, dr.diuretic_resis_date
         , ic.amountuom, CASE WHEN ic.amountuom like 'tsp' THEN amount/5
                              ELSE ic.amount END AS amount
  FROM inputevents_cv ic
       LEFT JOIN diuretic_resis dr
       ON dr.hadm_id = ic.hadm_id
 WHERE ic.amountuom::text like ANY (ARRAY['cc', 'ml', 'tsp'])
   AND ic.hadm_id = dr.hadm_id

UNION ALL
SELECT im.hadm_id, im.endtime AS charttime, dr.rrt, dr.dischtime, dr.diuretic_resis_date
       , im.amountuom, CASE WHEN im.amountuom like 'uL' THEN im.amount/1000
                            WHEN im.amountuom like 'ounces' THEN im.amount*29.27
                            ELSE im.amount END AS amount
  FROM inputevents_mv im
       LEFT JOIN diuretic_resis dr
       ON dr.hadm_id = im.hadm_id
 WHERE im.amountuom::text like ANY (ARRAY['ml', 'uL', 'ounces'])
   AND im.hadm_id = dr.hadm_id ) ip
WHERE charttime <= dischtime
GROUP BY hadm_id
)
, output_dis AS(
SELECT op.hadm_id
      , sum(op.amount) AS total_output_to_discharge
  FROM ( select op.hadm_id, op.charttime, value as amount, dr.rrt, dr.dischtime, dr.diuretic_resis_date
           from outputevents op
                left join diuretic_resis dr
                on op.hadm_id = dr.hadm_id
          where op.hadm_id = dr.hadm_id) op
 WHERE charttime <= dischtime
   GROUP BY op.hadm_id
)
, urine_output_rrt AS (
  SELECT dr.hadm_id
       , sum(value) AS urine_output_to_rrt
  FROM urineoutput uo
       LEFT JOIN icustay_detail icud
       ON uo.icustay_id = icud.icustay_id
       LEFT JOIN diuretic_resis dr
       ON icud.hadm_id = dr.hadm_id
 WHERE uo.icustay_id = icud.icustay_id
   AND icud.hadm_id = dr.hadm_id
   AND ((dr.rrt = 1 AND uo.charttime <= dr.rrt_charttime) OR (dr.rrt ISNULL AND uo.charttime <= (dr.diuretic_resis_date + '1 day'::interval day))) 
 GROUP BY dr.hadm_id
)
, urine_output_dis AS (
  SELECT dr.hadm_id
       , sum(value) AS urine_output_to_dis
  FROM urineoutput uo
       LEFT JOIN icustay_detail icud
       ON uo.icustay_id = icud.icustay_id
       LEFT JOIN diuretic_resis dr
       ON icud.hadm_id = dr.hadm_id
 WHERE uo.icustay_id = icud.icustay_id
   AND icud.hadm_id = dr.hadm_id
   AND uo.charttime < dr.dischtime
 GROUP BY dr.hadm_id
)
SELECT ir.subject_id
       , ir.hadm_id
       --, opt.total_output_to_rrt
       , (ir.total_input_to_rrt - opt.total_output_to_rrt) AS fluid_overload_to_rrt
       , CASE WHEN adm_to_rrt_day <> 0 THEN (ir.total_input_to_rrt - opt.total_output_to_rrt)/(adm_to_rrt_day) ELSE (ir.total_input_to_rrt - opt.total_output_to_rrt) END AS daily_fluid_overload_to_rrt
       , uo1.urine_output_to_rrt
       , CASE WHEN adm_to_rrt_day <> 0 THEN uo1.urine_output_to_rrt/(adm_to_rrt_day) ELSE uo1.urine_output_to_rrt END AS daily_uo_to_rrt
       , id.total_input_to_discharge
       , od.total_output_to_discharge
       , id.total_input_to_discharge - od.total_output_to_discharge AS fluid_overload_to_dis
       , uo2.urine_output_to_dis
  FROM input_rrt ir
       LEFT JOIN output_rrt opt
       ON opt.hadm_id = ir.hadm_id
       LEFT JOIN input_dis id
       ON id.hadm_id = ir.hadm_id
       LEFT JOIN output_dis od
       ON od.hadm_id = ir.hadm_id
       LEFT JOIN urine_output_rrt uo1
       ON uo1.hadm_id = ir.hadm_id
       LEFT JOIN urine_output_dis uo2
       ON uo2.hadm_id = ir.hadm_id
ORDER BY subject_id
