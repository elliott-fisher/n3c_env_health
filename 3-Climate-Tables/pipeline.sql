

@transform_pandas(
    Output(rid="ri.vector.main.execute.3fc965de-4a0c-453b-91c4-f37bcf27e18d"),
    CLIMATE_LAG_DIAGNOSIS_DATE=Input(rid="ri.foundry.main.dataset.590b3e52-6ac4-48dc-8af0-ae77a9d37eff")
)
SELECT count(distinct person_id)
FROM CLIMATE_LAG_DIAGNOSIS_DATE
where MeanTemp is not null and
      DewPoint is not null and
      Precip is not null

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c1402bec-1d8e-4416-bc81-beddda917d29"),
    CLIMATE_LAG_DIAGNOSIS_DATE=Input(rid="ri.foundry.main.dataset.590b3e52-6ac4-48dc-8af0-ae77a9d37eff"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810")
)
SELECT count(distinct a.person_id)
FROM COVID_POS_PERSON_FACT a
inner join
(
    select distinct person_id 
    from CLIMATE_LAG_DIAGNOSIS_DATE 
    where   MeanTemp is not null and
            DewPoint is not null and
            Precip is not null
) b
on a.person_id = b.person_id
where a.shift_date_yn = 'N'

