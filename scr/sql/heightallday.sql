CREATE MATERIALIZED VIEW heightallday AS WITH ce0 AS (
         SELECT c.icustay_id,
                CASE
                    WHEN (c.itemid = ANY (ARRAY[920, 1394, 4187, 3486])) THEN (c.valuenum * (2.54)::double precision)
                    ELSE c.valuenum
                END AS height
           FROM (mimiciii.chartevents c
             JOIN mimiciii.icustays ie_1 ON ((c.icustay_id = ie_1.icustay_id)))
          WHERE ((c.valuenum IS NOT NULL) AND (c.itemid = ANY (ARRAY[226730, 920, 1394, 4187, 3486, 3485, 4188])) AND (c.valuenum <> (0)::double precision))
        ), ce AS (
         SELECT ce0.icustay_id,
            percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ce0.height) AS height_chart
           FROM ce0
          WHERE (ce0.height > (100)::double precision)
          GROUP BY ce0.icustay_id
        ), echo AS (
         SELECT ec_1.subject_id,
            ((2.54)::double precision * percentile_cont((0.5)::double precision) WITHIN GROUP (ORDER BY ((ec_1.height)::double precision))) AS height_echo
           FROM (mimiciii.echodata ec_1
             JOIN mimiciii.icustays ie_1 ON ((ec_1.subject_id = ie_1.subject_id)))
          WHERE ((ec_1.height IS NOT NULL) AND ((ec_1.height * 2.54) > (100)::numeric))
          GROUP BY ec_1.subject_id
        )
 SELECT ie.icustay_id,
    COALESCE(ce.height_chart, ec.height_echo) AS height,
    ce.height_chart,
    ec.height_echo
   FROM (((mimiciii.icustays ie
     JOIN mimiciii.patients pat ON (((ie.subject_id = pat.subject_id) AND (ie.intime > (pat.dob + '1 year'::interval year)))))
     LEFT JOIN ce ON ((ie.icustay_id = ce.icustay_id)))
     LEFT JOIN echo ec ON ((ie.subject_id = ec.subject_id)));

