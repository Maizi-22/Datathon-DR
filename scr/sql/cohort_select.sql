-- This query select diuretic resis patients according to criteria:
--   1) drug dose criteria (diur_resis labeled 1 in diuretic_resis_raw)
--   2) Left Ventricular Systolic Function: as moderate or severe in echo data 
--      (lvef in echo_categorized labeled 2 or 3)
--   3) Not End Stage Renal Diseases
--
-- This query requires views:
--     1) rrt_all
--     2) echo_categorized
--     3) esrd_notes
--     4) diuretic_resis_raw
--
-- The selected variables includes:
--     1) id         : subject_id, hadm_id, 
--     2) demographic: age, gender
--     3) time info  : admition time, discharge time, icu intime, icu outtime, 
--                     diuretic resistance day, rrt dtart day, diuretic drug start day
--     4) duration   : los hospital, los icu, duration of diuretic resistance to rrt
--     5) therapy    : i) add dose therapy
--                     ii) change drug therapy
--                     iii) rrt
SET search_path TO mimiciii;
DROP MATERIALIZED VIEW IF EXISTS diuretic_resis CASCADE;
CREATE MATERIALIZED VIEW diuretic_resis AS WITH diu_resis_drug AS (
         --First DR record of patients who were diuretic resist
         SELECT drd.*
           FROM ( SELECT diur.*
                    -- In case a patient have more than 1 DR records
                    ,row_number() OVER (PARTITION BY diur.hadm_id ORDER BY diur.subject_id, diur.hadm_id, diur.startdate) AS resis_num
                   FROM mimiciii.diuretic_resis_raw diur
                  WHERE (diur.diur_resis = 1)
                  ORDER BY diur.subject_id, diur.startdate) drd
          WHERE ((drd.resis_num = 1) AND (drd.diur_resis = 1))
        )
         --When patient first diuretic resist
         , diur_resis_start AS (
         SELECT dd.subject_id,
            dd.hadm_id,
            dd.startdate,
            dd.druguse_seq
           FROM mimiciii.diuretic_resis_raw dd
          WHERE (dd.druguse_seq = 1)
        )
         --If patient change drug to improve diuretic resist, binary variables: 0 as no change, 1 as change drug
         , drug_change AS (
         SELECT dc.*
           FROM ( SELECT diuretic_resis_raw.subject_id,
                    diuretic_resis_raw.hadm_id,
                    diuretic_resis_raw.startdate AS change_startdate,
                    diuretic_resis_raw.drug AS drug_change,
                    row_number() OVER (PARTITION BY diuretic_resis_raw.hadm_id, diuretic_resis_raw.drug ORDER BY diuretic_resis_raw.subject_id, diuretic_resis_raw.hadm_id, diuretic_resis_raw.dose DESC) AS drug_change_rank
                   FROM mimiciii.diuretic_resis_raw
                  ORDER BY diuretic_resis_raw.subject_id, diuretic_resis_raw.hadm_id) dc
          WHERE (dc.drug_change_rank = 1)
        )
         --Max drug use record
         , max_drug AS (
         SELECT md.*
           FROM ( SELECT diuretic_resis_raw.subject_id,
                    diuretic_resis_raw.hadm_id,
                    diuretic_resis_raw.startdate AS maxdose_date,
                    diuretic_resis_raw.drug AS maxdrug,
                    diuretic_resis_raw.dose AS maxdose,
                    diuretic_resis_raw.unit AS maxunit,
                    diuretic_resis_raw.route AS maxroute,
                    row_number() OVER (PARTITION BY diuretic_resis_raw.hadm_id ORDER BY diuretic_resis_raw.subject_id, diuretic_resis_raw.hadm_id, diuretic_resis_raw.unit DESC) AS max_dose_rank
                   FROM mimiciii.diuretic_resis_raw
                  ORDER BY diuretic_resis_raw.subject_id, diuretic_resis_raw.hadm_id) md
          WHERE (md.max_dose_rank = 1)
        )
        --If patient (add drug/change drug) after diuretic resist
        , diuretic_resis_drug1 AS (
         SELECT DISTINCT dt.*,
            dds.startdate AS diuretic_drg_start,
            maxd.maxdose_date,
                CASE
                    WHEN ((dr.drug_change !~~ dt.drug) AND (dr.change_startdate >= dt.startdate)) THEN 1
                    ELSE NULL::integer
                END AS change_drug_therapy,
                CASE
                    WHEN ((maxd.maxdose > dt.dose) AND (maxd.maxdose_date > dt.startdate)) THEN 1
                    ELSE NULL::integer
                END AS add_drug_therapy
           FROM (diu_resis_drug dt
             LEFT JOIN drug_change dr ON (dr.hadm_id = dt.hadm_id)
             LEFT JOIN max_drug maxd ON (maxd.hadm_id = dt.hadm_id)
             LEFT JOIN diur_resis_start dds ON (dds.hadm_id = dt.hadm_id))
        )
         --If patient have change drug therapy, remove duplicate records where change_drug differs
         , diuretic_resis_drug AS (
         SELECT diuretic_resis_drug1.subject_id,
            diuretic_resis_drug1.hadm_id,
            diuretic_resis_drug1.drug,
            diuretic_resis_drug1.startdate,
            diuretic_resis_drug1.maxdose_date,
            diuretic_resis_drug1.dose,
            diuretic_resis_drug1.dose_unit_rx,
            diuretic_resis_drug1.route,
            diuretic_resis_drug1.diuretic_drg_start,
            diuretic_resis_drug1.add_drug_therapy,
            sum(diuretic_resis_drug1.change_drug_therapy) AS change_drug_therapy
           FROM diuretic_resis_drug1
          GROUP BY diuretic_resis_drug1.subject_id, diuretic_resis_drug1.hadm_id, diuretic_resis_drug1.drug, 
            diuretic_resis_drug1.maxdose_date, diuretic_resis_drug1.startdate, diuretic_resis_drug1.dose, diuretic_resis_drug1.dose_unit_rx, diuretic_resis_drug1.route, diuretic_resis_drug1.diuretic_drg_start, diuretic_resis_drug1.add_drug_therapy
        )
         --If patient have rrt 
         , rrt_after_dr AS (
         SELECT drd2.subject_id,
            drd2.hadm_id,
            drd2.rrt,
            drd2.charttime
           FROM ( SELECT drd.*,
                    rrt_all.rrt,
                    rrt_all.charttime,
                    row_number() OVER (PARTITION BY drd.hadm_id ORDER BY rrt_all.charttime) AS rrt_rank
                   FROM (diu_resis_drug drd
                     LEFT JOIN mimiciii.rrt_all ON (((rrt_all.subject_id = drd.subject_id) AND (rrt_all.hadm_id = drd.hadm_id) AND (rrt_all.rrt = 1) AND (rrt_all.charttime > drd.startdate))))) drd2
          WHERE (drd2.rrt_rank = 1)
        )
         -- Basic information fromicustay_detail
         , icustay_detail2 AS (
         SELECT icustay_detail.subject_id,
            icustay_detail.hadm_id,
            icustay_detail.gender,
            icustay_detail.admittime,
            icustay_detail.dischtime,
            icustay_detail.los_hospital,
            icustay_detail.age,
            icustay_detail.hospital_expire_flag,
            sum(((date_part('epoch'::text, (icustay_detail.outtime - icustay_detail.intime)) / (60)::double precision) / (60)::double precision)) AS los_icu,
            min(icustay_detail.intime) AS first_icu_intime,
            max(icustay_detail.outtime) AS last_icu_outtime
           FROM mimiciii.icustay_detail
          GROUP BY icustay_detail.subject_id, icustay_detail.hadm_id, icustay_detail.gender, icustay_detail.admittime, icustay_detail.dischtime, icustay_detail.los_hospital, icustay_detail.age, icustay_detail.hospital_expire_flag
        )
         -- Echo data preprocess, we would use lv_systolic in this data to identify if patient is diuretic resist
         , echo AS (
         SELECT ec.subject_id,
                CASE
                    WHEN (lower(ec.hadm_id) ~~ 'none'::text) THEN NULL::numeric
                    ELSE (ec.hadm_id)::numeric
                END AS hadm_id,
            (ec.new_time)::timestamp without time zone AS new_time,
                CASE
                    WHEN (ec."LV_systolic.txt" ~~ '{}'::text) THEN NULL::numeric
                    ELSE (ec."LV_systolic.txt")::numeric
                END AS lv_systolic
           FROM mimiciii.echo_categorized ec
        )
        -------------------------------------------------------------------------------------
        --- Identify when patient get better after DR (no rrt)
        --- Get better definition: Urine output increase after patients reached max drug dose
        --- How to identify 'Urine output increase'? Two ways:
        ---    a ) urine output overweights that day before and fluid input > output
        ---    b ) patient's weight decrease than before
        -------------------------------------------------------------------------------------
        --- Daily fluid input from inputevents_cv and inputevents_mv
        , input24 AS (
        SELECT ip.hadm_id, ip.chartdate, sum(ip.amount) AS input_24h
          FROM ( SELECT ic.hadm_id, date(ic.charttime) AS chartdate, maxdose_date
                        , CASE WHEN ic.amountuom like 'tsp' THEN amount/5            -- convert tsp to ml
                               ELSE ic.amount END AS amount
                   FROM inputevents_cv ic
                        LEFT JOIN  diuretic_resis_drug dr
                        ON dr.hadm_id = ic.hadm_id
                  WHERE ic.amountuom::text like ANY (ARRAY['cc', 'ml', 'tsp'])       -- three fluid unit in this table
                    AND ic.hadm_id = dr.hadm_id
                 UNION ALL
                 SELECT im.hadm_id, date(im.endtime) AS chartdate, maxdose_date
                        , CASE WHEN im.amountuom like 'uL' THEN im.amount/1000       -- convert nL to ml
                               WHEN im.amountuom like 'ounces' THEN im.amount*29.27  -- convert ounces to ml
                               ELSE im.amount END AS amount
                   FROM inputevents_mv im
                        LEFT JOIN  diuretic_resis_drug dr
                        ON dr.hadm_id = im.hadm_id 
                  WHERE im.amountuom::text like ANY (ARRAY['ml', 'uL', 'ounces'])    -- three fluid unit in this table
                    AND im.hadm_id = dr.hadm_id ) ip
          GROUP BY ip.hadm_id, ip.chartdate
         )
         -- Daily fluid output from outputevents
         , output24 AS (
         SELECT op.hadm_id, op.chartdate
                , sum(op.amount) AS output_24h
           FROM ( select op.hadm_id, date(op.charttime) AS chartdate, value as amount
                    from outputevents op
                         left join  diuretic_resis_drug dr
                         on op.hadm_id = dr.hadm_id
                   where op.hadm_id = dr.hadm_id) op
            GROUP BY op.hadm_id, op.chartdate
         )
         -- Daily urine output 
         , uo24 AS (
           SELECT dr.hadm_id, DATE(charttime) AS chartdate
                  , sum(value) AS urine_output_24h
             FROM urineoutput uo
                  LEFT JOIN icustay_detail icud
                  ON uo.icustay_id = icud.icustay_id
                  LEFT JOIN  diuretic_resis_drug dr
                  ON icud.hadm_id = dr.hadm_id
            WHERE uo.icustay_id = icud.icustay_id
              AND icud.hadm_id = dr.hadm_id
            GROUP BY dr.hadm_id, chartdate
         )
         -- Daily urine output one day early
         , uo24_early AS (
           SELECT dr.hadm_id, DATE((charttime) - '1 day'::interval day) AS chartdate
                  , sum(value) AS urine_output_24h_early
             FROM urineoutput uo
                  LEFT JOIN icustay_detail icud
                  ON uo.icustay_id = icud.icustay_id
                  LEFT JOIN diuretic_resis_drug dr
                  ON icud.hadm_id = dr.hadm_id
            WHERE uo.icustay_id = icud.icustay_id
              AND icud.hadm_id = dr.hadm_id
            GROUP BY dr.hadm_id, chartdate
         )
         -- Patient's weight
         , we AS (
           SELECT dr.subject_id
                , dr.hadm_id
                , dr.maxdose_date
                , c.charttime AS weight_charttime
                -- One wronged record need to convert
                , CASE WHEN c.icustay_id = '276915' AND c.valuenum = 139 THEN c.valuenum/2 ELSE c.valuenum END AS weight
           FROM diuretic_resis_drug dr
                LEFT JOIN icustays icus
                ON  icus.hadm_id = dr.hadm_id
                LEFT JOIN mimiciii.chartevents c
                ON c.icustay_id = icus.icustay_id
          WHERE ((c.valuenum IS NOT NULL)
            AND (c.itemid = ANY (ARRAY[762, 226512, 763, 224639]))
            AND c.charttime >= maxdose_date
            AND (c.valuenum <> (0)::double precision)
            AND (c.error IS DISTINCT FROM 1))
         )
         -- Criterion a: - If patient weight decrease: if a value smaller than last record
         ----------------- eg: values are: 69, 72, 71, 73
         --------------------- we would like to find 71 and its record time
         , we2 AS (
           SELECT *
                , row_number() OVER (PARTITION BY wr.hadm_id ORDER BY ref_time) AS rank3
           FROM ( SELECT DISTINCT we.*
                , we2.weight as weight2
                , we2.weight_charttime AS weight_charttime2
                , CASE WHEN we2.weight_charttime >= we.weight_charttime AND we2.weight < we.weight THEN we2.weight_charttime END AS ref_time
                -- charttime rank
                , row_number() OVER (PARTITION BY we.hadm_id ORDER BY we.weight_charttime) AS rank1
                -- charttime rank after join itself
                , row_number() OVER (PARTITION BY we.hadm_id, we.weight_charttime ORDER BY we2.weight_charttime) AS rank2
           FROM we
                -- Left join the table itself
                LEFT JOIN we we2
                ON we.hadm_id = we2.hadm_id
           ) wr
          WHERE wr.rank2 <= wr.rank1  -- compare rank to find the next value bigger than before
            AND ref_time IS NOT NULL
         )
         -- Use criterion b to identify urine increase
         , fb AS(
         SELECT re.*
                , row_number() over(partition by re.hadm_id order by re.ref_date) AS seq
           FROM ( SELECT distinct dr.subject_id
                         , dr.hadm_id
                         , dr.maxdose_date
                         , CASE WHEN ip.input_24h < op.output_24h AND ue.urine_output_24h_early > uo.urine_output_24h THEN ip.chartdate END AS ref_date
                    FROM diuretic_resis_drug dr
                         LEFT JOIN input24 ip
                         ON ip.hadm_id = dr.hadm_id
                         LEFT JOIN output24 op
                         ON op.hadm_id = dr.hadm_id
                         AND op.chartdate = ip.chartdate
                         LEFT JOIN uo24 uo
                         ON uo.hadm_id = dr.hadm_id
                         AND uo.chartdate = ip.chartdate
                         LEFT JOIN uo24_early ue
                         ON ue.hadm_id = dr.hadm_id
                         AND ue.chartdate = ip.chartdate
                   WHERE ip.chartdate >= maxdose_date
                   ORDER BY dr.subject_id, dr.hadm_id, ref_date ) re
         )
         ---------------------
         ---------------------
         ---------------------
         -- Finally got a complete dataset of diuretic resis patients!
         , diuretic_final AS (
         SELECT DISTINCT dr.*
                , CASE WHEN ref_date <= we.ref_time OR we.ref_time IS NULL THEN ref_date
                       ELSE ref_time END AS get_better_date
                , rrt.charttime as rrt_charttime
           FROM diuretic_resis_drug dr
                LEFT JOIN fb
                ON fb.hadm_id = dr.hadm_id
                LEFT JOIN (SELECT subject_id, hadm_id, ref_time
                             FROM we2
                            WHERE rank3 = 1) we
                ON we.hadm_id = fb.hadm_id
                LEFT JOIN echo ON (((echo.subject_id = dr.subject_id) AND (echo.hadm_id = (dr.hadm_id)::numeric)))
                LEFT JOIN mimiciii.esrd_notes en ON ((en.hadm_id = dr.hadm_id))
                LEFT JOIN rrt_after_dr rrt ON (((rrt.subject_id = dr.subject_id) AND (rrt.hadm_id = dr.hadm_id)))
                LEFT JOIN mimiciii.diagnoses_icd dicd ON (((dicd.subject_id = dr.subject_id) AND (dicd.hadm_id = dr.hadm_id)))
                LEFT JOIN icustay_detail2 icud ON (icud.hadm_id = dr.hadm_id)
          WHERE (seq = 1 OR seq is null)
            AND echo.lv_systolic = ANY (ARRAY[(2)::numeric, (3)::numeric])
            AND (en.esrd_notes = 0)
            --AND (rrt.charttime >= dr.startdate or rrt.charttime is null)
            AND ((dicd.icd9_code)::text ~~ ANY (ARRAY['39891'::text, '40201'::text, '40211'::text, '40291'::text, '40401'::text, '40411'::text, '40491'::text, '4280'::text, '4281'::text, '42820'::text, '42821'::text, '42822'::text, '42823'::text, '4289'::text]))
            AND (icud.age >= (18)::numeric)    
          ORDER BY subject_id
         )
        --- Total drug use before rrt/urine increase
        , drug_unit_2rrt AS (
         SELECT DISTINCT dr.subject_id,
            dr.hadm_id,
            sum(drr.unit) AS total_drug_unit
           FROM diuretic_final dr
             LEFT JOIN mimiciii.diuretic_resis_raw drr ON dr.hadm_id = drr.hadm_id
          WHERE rrt_charttime IS NOT NULL AND (dr.rrt_charttime >= drr.startdate)
          GROUP BY dr.subject_id, dr.hadm_id
          UNION ALL
          SELECT DISTINCT dr.subject_id,
            dr.hadm_id,
            sum(drr.unit) AS total_drug_unit
           FROM diuretic_final dr
             LEFT JOIN mimiciii.diuretic_resis_raw drr ON dr.hadm_id = drr.hadm_id
          WHERE rrt_charttime IS NULL AND (dr.get_better_date >= drr.startdate)
          GROUP BY dr.subject_id, dr.hadm_id
        )
        
        ---Cohort
        SELECT df.subject_id
               , df.hadm_id
               , icud.age
               , icud.gender
               , icud.admittime
               , icud.dischtime
               , icud.hospital_expire_flag
               , adm.deathtime
               , ((date_part('epoch'::text, (icud.dischtime - icud.admittime)) / (60)::double precision) / (60)::double precision) AS los_hos
               , icud.first_icu_intime
               , icud.last_icu_outtime
               , icud.los_icu
               , df.startdate AS diuretic_resis_date
               , df.diuretic_drg_start
               , df.maxdose_date
               , df.add_drug_therapy
               , df.change_drug_therapy
               , drr2.total_drug_unit
               , df.get_better_date
               , rrt.rrt
               , rrt.charttime AS rrt_charttime
               , CASE
                    WHEN ((rrt.charttime >= icud.first_icu_intime) AND (rrt.charttime <= icud.last_icu_outtime) AND (rrt.charttime > df.startdate)) THEN date_part('day'::text, (rrt.charttime - (df.startdate)::timestamp without time zone))
                    WHEN rrt.charttime IS NULL AND df.get_better_date >= df.startdate THEN date_part('day'::text, (df.get_better_date - (df.startdate)::timestamp without time zone))
                    ELSE NULL::double precision
                    END AS diure_resis_to_rrt_day
               , CASE
                    WHEN ((rrt.charttime >= icud.first_icu_intime) AND (rrt.charttime <= icud.last_icu_outtime) AND (rrt.charttime > df.startdate)) THEN date_part('day'::text, (rrt.charttime - (df.diuretic_drg_start)::timestamp without time zone))
                    WHEN rrt.charttime IS NULL AND df.get_better_date >= df.startdate THEN date_part('day'::text, (df.get_better_date - (df.diuretic_drg_start)::timestamp without time zone))
                    ELSE NULL::double precision
                    END AS diure_drug_use_to_rrt_day
          FROM diuretic_final df
               LEFT JOIN icustay_detail2 icud ON icud.hadm_id = df.hadm_id
               LEFT JOIN mimiciii.admissions adm ON adm.hadm_id = df.hadm_id
               LEFT JOIN rrt_after_dr rrt ON rrt.hadm_id = df.hadm_id
               LEFT JOIN drug_unit_2rrt drr2 ON drr2.hadm_id = df.hadm_id
         ORDER BY df.subject_id
