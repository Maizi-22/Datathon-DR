-- Materialized View: mimiciii.rrt_all

-- DROP MATERIALIZED VIEW mimiciii.rrt_all;

CREATE MATERIALIZED VIEW mimiciii.rrt_all AS 
 WITH cv AS (
         SELECT ie_1.icustay_id,
            max(
                CASE
                    WHEN (ce.itemid = ANY (ARRAY[152, 148, 149, 146, 147, 151, 150])) AND ce.value IS NOT NULL THEN 1
                    WHEN (ce.itemid = ANY (ARRAY[229, 235, 241, 247, 253, 259, 265, 271])) AND ce.value::text = 'Dialysis Line'::text THEN 1
                    WHEN ce.itemid = 582 AND (ce.value::text = ANY (ARRAY['CAVH Start'::character varying::text, 'CAVH D/C'::character varying::text, 'CVVHD Start'::character varying::text, 'CVVHD D/C'::character varying::text, 'Hemodialysis st'::character varying::text, 'Hemodialysis end'::character varying::text])) THEN 1
                    ELSE 0
                END) AS rrt,
            ce.charttime
           FROM mimiciii.icustays ie_1
             JOIN mimiciii.chartevents ce ON ie_1.icustay_id = ce.icustay_id AND (ce.itemid = ANY (ARRAY[152, 148, 149, 146, 147, 151, 150, 229, 235, 241, 247, 253, 259, 265, 271, 582])) AND ce.value IS NOT NULL
          WHERE ie_1.dbsource::text = 'carevue'::text
          GROUP BY ie_1.icustay_id, ce.charttime
        ), mv_ce AS (
         SELECT ie_1.icustay_id,
            1 AS rrt
           FROM mimiciii.icustays ie_1
             JOIN mimiciii.chartevents ce ON ie_1.icustay_id = ce.icustay_id AND ce.charttime >= ie_1.intime AND (ce.itemid = ANY (ARRAY[226118, 227357, 225725, 226499, 224154, 225810, 227639, 225183, 227438, 224191, 225806, 225807, 228004, 228005, 228006, 224144, 224145, 224149, 224150, 224151, 224152, 224153, 224404, 224406, 226457])) AND ce.valuenum > 0::double precision
          GROUP BY ie_1.icustay_id
        ), mv_ie AS (
         SELECT ie_1.icustay_id,
            1 AS rrt
           FROM mimiciii.icustays ie_1
             JOIN mimiciii.inputevents_mv tt ON ie_1.icustay_id = tt.icustay_id AND tt.starttime >= ie_1.intime AND (tt.itemid = ANY (ARRAY[227536, 227525])) AND tt.amount > 0::double precision
          GROUP BY ie_1.icustay_id
        ), mv_de AS (
         SELECT ie_1.icustay_id,
            1 AS rrt
           FROM mimiciii.icustays ie_1
             JOIN mimiciii.datetimeevents tt ON ie_1.icustay_id = tt.icustay_id AND tt.charttime >= ie_1.intime AND (tt.itemid = ANY (ARRAY[225318, 225319, 225321, 225322, 225324]))
          GROUP BY ie_1.icustay_id
        ), mv_pe AS (
         SELECT ie_1.icustay_id,
            1 AS rrt
           FROM mimiciii.icustays ie_1
             JOIN mimiciii.procedureevents_mv tt ON ie_1.icustay_id = tt.icustay_id AND tt.starttime >= ie_1.intime AND (tt.itemid = ANY (ARRAY[225441, 225802, 225803, 225805, 224270, 225809, 225955, 225436]))
          GROUP BY ie_1.icustay_id
        )
 SELECT ie.subject_id,
    ie.hadm_id,
    ie.icustay_id,
    cv.charttime,
        CASE
            WHEN cv.rrt = 1 THEN 1
            WHEN mv_ce.rrt = 1 THEN 1
            WHEN mv_ie.rrt = 1 THEN 1
            WHEN mv_de.rrt = 1 THEN 1
            WHEN mv_pe.rrt = 1 THEN 1
            ELSE 0
        END AS rrt
   FROM mimiciii.icustays ie
     LEFT JOIN cv ON ie.icustay_id = cv.icustay_id
     LEFT JOIN mv_ce ON ie.icustay_id = mv_ce.icustay_id
     LEFT JOIN mv_ie ON ie.icustay_id = mv_ie.icustay_id
     LEFT JOIN mv_de ON ie.icustay_id = mv_de.icustay_id
     LEFT JOIN mv_pe ON ie.icustay_id = mv_pe.icustay_id
  ORDER BY ie.subject_id, cv.charttime
WITH DATA;

ALTER TABLE mimiciii.rrt_all
  OWNER TO postgres;

