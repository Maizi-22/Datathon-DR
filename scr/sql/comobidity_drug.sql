-- This code extract drug use and comobidity for diuretic resist patients
-- Drug use includes four types: BBR, inotropes_vasopressor, ACEI/ARB, Vasodilator
-- Comobidity includes :Ischemic Heart Disease
--                      Cardiomyopathies
--                      Valvular Disease
--                      Atrial Fibrilation or Atrial Flutter
--                      Hyperlipidemia
--                      Hypertension
--                      Diabetes
--                      Sleep Disordered Breathing
--                      Renal failure
--                      Anemia
--                      Hypoproteinemia
--                      Infections
--                      Alcohol abuse*

-- This query requires view: elixhauser_ahrq

SET search_path TO mimiciii;

-- drug use
WITH drug1 AS(
  SELECT subject_id
                 , hadm_id
                 , icustay_id
                 , CASE WHEN lower(pr.drug)::text LIKE ANY (ARRAY['%losartan%', '%cozaar%', '%valsartan%', '%diovan%', '%cilexetil%', '%micardis%', '%olmesartan%']) THEN 1 END AS ARB
                 , CASE WHEN lower(pr.drug)::text LIKE ANY (ARRAY['%capoten%', '%enalapril%', '%enam%', '%renitec%', '%benazepril%', '%lotensin%', '%quinapril%', '%altace%', '%lisinopril%', '%alapril%', '%zestril%', '%fosinopril%', '%Monopril%']) THEN 1 END AS ACEI
                 , CASE WHEN lower(pr.drug)::text LIKE ANY (ARRAY['%metoprolol%', '%succinate%', '%tartrate%', '%toprol%', '%atenolol%', '%esmolol%', '%carvedilol%', '%bisoprolol%', '%trandol%', '%propranolol%', '%inderal%', '%acebutolol%', '%nadolol%']) THEN 1 END AS BBR
                 , CASE WHEN lower(pr.drug)::text LIKE ANY (ARRAY['%spironolactone%', '%eplerenone%']) THEN 1 END AS MRA
	               , CASE WHEN lower(pr.drug)::text LIKE ANY (ARRAY['%digoxin%', '%digaoxin%', '%digoxine%', '%digoxinum%', '%lanoxin%', '%milrinone%', '%corotrop%', '%corotrope%', '%milrinone lactate%', '%milrinonum%', '%primacor%', '%amrinone%', '%dobutamine%', '%dobuject%', '%doburex%', '%dobutaminum%', '%dobutrex%', '%inotrex%', '%dopamine%', '%abbodop%', '%cardiosteril%', '%catabon%', '%dopamed%', '%dopamine hydrobromide%', '%dopamine hydrochloride%', '%dopaminum%', '%dopastat%', '%dopmin%', '%inoban%', '%intropin%', '%revimine%', '%revivan%', '%adrenalin%','%epinephrine%', '%phenylephrine%', '%adrianol%', '%biomydrin%', '%isophrin%', '%mesatonum%', '%metaoxedrinum%', '%mydfrin%', '%neo-oxedrine%', '%neosynephrine%', '%phenvlenhrine%', '%phenylephdrine%', '%phenylephdrine hydrochloride%', '%phenylephdrinum%', '%phenylephrine hydrochloride%', '%visopt%', '%norepinephrine%', '%noradrenaline%', '%aramine%', '%metaraminol%', '%pimobendan%']) THEN 1 END AS Inotropes_vasopressors
	               , CASE WHEN lower(pr.drug)::text LIKE ANY (ARRAY['%isordil%', '%nitroglycerin%']) THEN 1 END AS vasodilator
     FROM prescriptions pr
	 WHERE lower(pr.drug)::text LIKE ANY (ARRAY['%metoprolol%', '%succinate%', '%tartrate%', '%toprol%', '%atenolol%', '%esmolol%', '%carvedilol%', '%bisoprolol%', '%trandol%', '%propranolol%', '%inderal%', '%acebutolol%', '%nadolol%', '%digoxin%', '%digaoxin%', '%digoxine%', '%digoxinum%', '%lanoxin%', '%milrinone%', '%corotrop%', '%corotrope%', '%milrinone lactate%', '%milrinonum%', '%primacor%', '%amrinone%', '%dobutamine%', '%dobuject%', '%doburex%', '%dobutaminum%', '%dobutrex%', '%inotrex%', '%dopamine%', '%abbodop%', '%cardiosteril%', '%catabon%', '%dopamed%', '%dopamine hydrobromide%', '%dopamine hydrochloride%', '%dopaminum%', '%dopastat%', '%dopmin%', '%inoban%', '%intropin%', '%revimine%', '%revivan%', '%adrenalin%','%epinephrine%', '%phenylephrine%', '%adrianol%', '%biomydrin%', '%isophrin%', '%mesatonum%', '%metaoxedrinum%', '%mydfrin%', '%neo-oxedrine%', '%neosynephrine%', '%phenvlenhrine%', '%phenylephdrine%', '%phenylephdrine hydrochloride%', '%phenylephdrinum%', '%phenylephrine hydrochloride%', '%visopt%', '%norepinephrine%', '%noradrenaline%', '%aramine%', '%metaraminol%', '%pimobendan%', '%capoten%', '%enalapril%', '%enam%', '%renitec%', '%benazepril%', '%lotensin%', '%quinapril%', '%altace%', '%lisinopril%', '%alapril%', '%zestril%', '%fosinopril%', '%Monopril%', '%isordil%', '%nitroglycerin%', '%spironolactone%', '%eplerenone%'])
), drug2 AS (
SELECT distinct subject_id,
       hadm_id,
			 sum(bbr) as bbr,
			 sum(inotropes_vasopressors) as inotropes_vasopressor,
			 sum(acei) as acei,
			 sum(arb) as arb,
                         sum(mra) as mra,
	     sum(vasodilator) as vasodilator
  from drug1
 --where bbr is not null or Inotropes_vasopressor is not null or acei is not null or arb is not null
 group by subject_id,
       hadm_id
)

-- comobidity
, cmbdt AS (
  SELECT subject_id
       , ea.hadm_id
	     , ischemic_heart_disease
	     , cardiomyopathies
	     , vd.valvular_disease
	     , atrial_fibrilation
	     , hyperlipidemia
       , hypertension
       , CASE WHEN diabetes_uncomplicated = 1 OR diabetes_complicated = 1 THEN 1 ELSE 0 END AS diabetes
	     , sleep_disordered_breathing
       , renal_failure
       , anemia
       , infection
       , alcohol_abuse
  FROM elixhauser_ahrq ea
LEFT JOIN (
      SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN ("substring"((dicd.icd9_code)::text, 1, 3) = ANY (ARRAY['001'::text, '002'::text, '003'::text, '004'::text, '005'::text, '008'::text, '009'::text, '010'::text, '011'::text, '012'::text, '013'::text, '014'::text, '015'::text, '016'::text, '017'::text, '018'::text, '020'::text, '021'::text, '022'::text, '023'::text, '024'::text, '025'::text, '026'::text, '027'::text, '030'::text, '031'::text, '032'::text, '033'::text, '034'::text, '035'::text, '036'::text, '037'::text, '038'::text, '039'::text, '040'::text, '041'::text, '090'::text, '091'::text, '092'::text, '093'::text, '094'::text, '095'::text, '096'::text, '097'::text, '098'::text, '100'::text, '101'::text, '102'::text, '103'::text, '104'::text, '110'::text, '111'::text, '112'::text, '114'::text, '115'::text, '116'::text, '117'::text, '118'::text, '320'::text, '322'::text, '324'::text, '325'::text, '420'::text, '421'::text, '451'::text, '461'::text, '462'::text, '463'::text, '464'::text, '465'::text, '481'::text, '482'::text, '485'::text, '486'::text, '494'::text, '510'::text, '513'::text, '540'::text, '541'::text, '542'::text, '566'::text, '567'::text, '590'::text, '597'::text, '601'::text, '614'::text, '615'::text, '616'::text, '681'::text, '682'::text, '683'::text, '686'::text, '730'::text])) THEN 1
                    WHEN ("substring"((dicd.icd9_code)::text, 1, 4) = ANY (ARRAY['5695'::text, '5720'::text, '5721'::text, '5750'::text, '5990'::text, '7110'::text, '7907'::text, '9966'::text, '9985'::text, '9993'::text])) THEN 1
                    WHEN ("substring"((dicd.icd9_code)::text, 1, 5) = ANY (ARRAY['49121'::text, '56201'::text, '56203'::text, '56211'::text, '56213'::text, '56983'::text])) THEN 1
                    ELSE 0
                END AS infection
           FROM mimiciii.diagnoses_icd dicd
      ) inf
    ON inf.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN (icd9_code >= '410'::bpchar) AND (icd9_code <= '41407'::bpchar) THEN 1
									  WHEN (icd9_code >= '4142'::bpchar) AND (icd9_code <= '4149'::bpchar) THEN 1
									  WHEN icd9_code = '42979' THEN 1 ELSE 0
                END AS ischemic_heart_disease
           FROM mimiciii.diagnoses_icd dicd
			) ihd
		 ON ihd.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN (icd9_code >= '42511'::bpchar) AND (icd9_code <= '4259'::bpchar) THEN 1
									  WHEN icd9_code = '41982' THEN 1
									  WHEN icd9_code = '4293' THEN 1 ELSE 0
                END AS cardiomyopathies
           FROM mimiciii.diagnoses_icd dicd
			) ca
		 ON ca.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN (icd9_code >= '394'::bpchar) AND (icd9_code <= '3971'::bpchar) THEN 1
									  WHEN icd9_code = '932%' THEN 1
									  WHEN icd9_code = '424%' THEN 1
									  WHEN (icd9_code >= '7460'::bpchar) AND (icd9_code <= '7466'::bpchar) THEN 1
									  WHEN icd9_code = 'V422' THEN 1
									  WHEN icd9_code = 'V433' THEN 1
									  WHEN icd9_code = '99602' THEN 1
									  WHEN icd9_code = '99671' THEN 1 ELSE 0
                END AS valvular_disease
           FROM mimiciii.diagnoses_icd dicd
			) vd
		 ON vd.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN (icd9_code >= '42731'::bpchar) AND (icd9_code <= '42732'::bpchar) THEN 1 ELSE 0
                END AS atrial_fibrilation
           FROM mimiciii.diagnoses_icd dicd
			) af
		 ON af.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN (icd9_code >= '2720'::bpchar) AND (icd9_code <= '2724'::bpchar) THEN 1 ELSE 0
                END AS hyperlipidemia
           FROM mimiciii.diagnoses_icd dicd
			) hld
		 ON hld.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN (icd9_code >= '32720'::bpchar) AND (icd9_code <= '32723'::bpchar) THEN 1
									  WHEN icd9_code in ('32727', '32729', '78051', '78053', '78057') THEN 1 ELSE 0
                END AS sleep_disordered_breathing
           FROM mimiciii.diagnoses_icd dicd
			) sdb
		 ON sdb.hadm_id = ea.hadm_id
LEFT JOIN (
			SELECT DISTINCT dicd.hadm_id,
                CASE
                    WHEN ((icd9_code >= '280 '::bpchar) AND (icd9_code <= '2819 '::bpchar)) THEN 1
                    WHEN ((icd9_code >= '2822'::bpchar) AND (icd9_code <= '28319'::bpchar)) THEN 1
                    WHEN (icd9_code = '2839 '::bpchar) THEN 1
									  WHEN ((icd9_code >= '28409 '::bpchar) AND (icd9_code <= '28419 '::bpchar)) THEN 1
                    WHEN ((icd9_code >= '2848'::bpchar) AND (icd9_code <= '2859'::bpchar)) THEN 1
                    WHEN ((icd9_code >= '64820'::bpchar) AND (icd9_code <= '64824'::bpchar)) THEN 1 ELSE 0
                END AS anemia
           FROM mimiciii.diagnoses_icd dicd
			) ane
		 ON ane.hadm_id = ea.hadm_id
)
, cmbdt2 AS(
	SELECT cm.subject_id
	       , cm.hadm_id
	       , SUM(ischemic_heart_disease) AS ischemic_heart_disease
	       , SUM(cardiomyopathies) AS cardiomyopathies
	       , SUM(valvular_disease) AS valvular_disease
	       , SUM(atrial_fibrilation) AS atrial_fibrilation
	       , SUM(hyperlipidemia) AS hyperlipidemia
	       , SUM(hypertension) AS hypertension
	       , SUM(diabetes) AS diabetes
	       , SUM(sleep_disordered_breathing) AS sleep_disordered_breathing
	       , SUM(renal_failure) AS renal_failure
	       , SUM(anemia) AS anemia
	       , SUM(infection) AS infection
	       , SUM(alcohol_abuse) AS alcohol_abuse
	  FROM cmbdt cm
	GROUP BY cm.subject_id
	       , cm.hadm_id
)

SELECT DISTINCT dr.subject_id
       , dr.hadm_id
       , CASE WHEN bbr <> 0 THEN 1 ELSE 0 END AS bbr
       , CASE WHEN inotropes_vasopressor <> 0 THEN 1 ELSE 0 END AS inotropes_vasopressor
       , CASE WHEN acei <> 0 THEN 1 ELSE 0 END AS acei
       , CASE WHEN mra <> 0 THEN 1 ELSE 0 END AS mra
       , CASE WHEN vasodilator <> 0 THEN 1 ELSE 0 END AS vasodilator
       , CASE WHEN cm.ischemic_heart_disease <> 0 THEN 1 ELSE 0 END AS ischemic_heart_disease
       , CASE WHEN cm.cardiomyopathies <> 0 THEN 1 ELSE 0 END AS cardiomyopathies
       , CASE WHEN cm.valvular_disease <> 0 THEN 1 ELSE 0 END AS valvular_disease
       , CASE WHEN cm.atrial_fibrilation <> 0 THEN 1 ELSE 0 END AS atrial_fibrilation
       , CASE WHEN cm.hyperlipidemia <> 0 THEN 1 ELSE 0 END AS hyperlipidemia
       , CASE WHEN cm.hypertension <> 0 THEN 1 ELSE 0 END AS hypertension
       , CASE WHEN cm.diabetes <> 0 THEN 1 ELSE 0 END AS diabetes
       , CASE WHEN cm.sleep_disordered_breathing <> 0 THEN 1 ELSE 0 END AS sleep_disordered_breathing
       , CASE WHEN cm.renal_failure <> 0 THEN 1 ELSE 0 END AS renal_failure
       , CASE WHEN cm.anemia <> 0 THEN 1 ELSE 0 END AS anemia
       , CASE WHEN cm.infection <> 0 THEN 1 ELSE 0 END AS infection
       , CASE WHEN cm.alcohol_abuse <> 0 THEN 1 ELSE 0 END AS alcohol_abuse
  FROM diuretic_resis dr
       LEFT JOIN drug2
		   ON drug2.hadm_id = dr.hadm_id
       LEFT JOIN cmbdt2 cm
       ON cm.hadm_id = dr.hadm_id
