

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.99269dbe-20ed-4527-b65b-648aef30796b"),
    ZCTA_by_SDoH_percentages=Input(rid="ri.foundry.main.dataset.f6ce6698-905f-445b-a7b6-4b3894d73d61"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
SELECT ZCTA_by_SDoH_percentages.*,ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZIP_CODE as zipcode
FROM ZCTA_by_SDoH_percentages JOIN ZiptoZcta_Crosswalk_2021_ziptozcta2020 on ZCTA_by_SDoH_percentages.ZCTA=ZiptoZcta_Crosswalk_2021_ziptozcta2020.ZCTA

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.27140daf-f64a-4271-9928-739d4c94d84e"),
    pf_sdoh=Input(rid="ri.foundry.main.dataset.c9872f5f-e024-4889-b8f0-11e02fb1a21d")
)
SELECT distinct city,state,zip,ZCTA
FROM pf_sdoh
where ZCTA IS NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ef1715b2-121f-4129-86c2-e8cc3759e432"),
    LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_=Input(rid="ri.foundry.main.dataset.75d7da57-7b0e-462c-b41d-c9ef4f756198"),
    pf_sdoh=Input(rid="ri.foundry.main.dataset.c9872f5f-e024-4889-b8f0-11e02fb1a21d")
)
SELECT pf_sdoh.*, LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_.MDs

FROM pf_sdoh LEFT JOIN LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_ on pf_sdoh.person_id=LOGIC_LIAISON_Covid_19_Patient_Summary_Facts_Table_LDS_.person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c9872f5f-e024-4889-b8f0-11e02fb1a21d"),
    ZCTA_by_SDOH_wZIP=Input(rid="ri.foundry.main.dataset.99269dbe-20ed-4527-b65b-648aef30796b"),
    pf_death=Input(rid="ri.foundry.main.dataset.b0eef012-2041-4a78-9289-610f2661f10a")
)
SELECT pf_death.zip_code, pf_death.person_id, ZCTA_by_SDOH_wZIP.*
FROM pf_death LEFT JOIN ZCTA_by_SDOH_wZIP on pf_death.zip_code=ZCTA_by_SDOH_wZIP.zipcode

