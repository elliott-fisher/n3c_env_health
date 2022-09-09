from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903"),
    covid_diag_recs=Input(rid="ri.foundry.main.dataset.32a1b5af-51ef-4b7b-acdd-344a94656be6"),
    covid_pos_recs=Input(rid="ri.foundry.main.dataset.7f6f5a78-e79a-4c4d-8549-75f919d84cdc")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:
Creates a table of distinct Covid+ patients with dates of the first positive PCR 
or Antigen lab results.

By switching the USE_POSLAB_ONLY boolean value to False, patients with positive 
COVID diagnoses and positive antibody tests results are included.

Input:

Output:
================================================================================
"""
def ALL_COVID_POS_PATIENTS(covid_pos_recs, covid_diag_recs):

    #If set to True, then keeps only PCR /Antigen positive test as COVID_indicator
    USE_POSLAB_ONLY = True

    # reduce number of covid_pos_recs columns and create friendlier names for concept set names     
    lab_measurements = covid_pos_recs.select('person_id', 'measurement_date', 'concept_set_name')
    lab_measurements = (
        lab_measurements
            .withColumn('covid_event_type', 
                F.when(F.col('concept_set_name') == 'ATLAS SARS-CoV-2 rt-PCR and AG', F.lit('PCR or Antigen Test'))
                    .otherwise(F.when(F.col('concept_set_name') == 'Atlas #818 [N3C] CovidAntibody retry', F.lit('Antibody Test'))
                        .otherwise(F.lit('Other lab measurement')))
            )
    )

    # Gets first positive lab measurement date for PCR/Antigen and/or Antibody
    # Note: this table can only have up to two record per person - one each for PCR/Antigen and/or Antibody   
    lab_measurements = lab_measurements.groupBy('person_id', 'covid_event_type').agg(F.min('measurement_date').alias('first_diagnosis_date'))
    lab_measurements = lab_measurements.withColumn('diagnosis_type', F.when(F.col('covid_event_type') == 'PCR or Antigen Test', F.lit(1)).otherwise(F.lit(4)))
    

    # reduce number of covid_diag_recs columns and create friendlier name for concept set name 
    conditions = covid_diag_recs.select('person_id', 'condition_start_date', 'concept_set_name')
    conditions = (
        conditions
            .withColumn('covid_event_type', F.when(F.col('concept_set_name') == 'N3C Covid Diagnosis', F.lit('Condition Diagnosis'))
            .otherwise(F.lit('Other Condition')))
    )  

    # Gets first Covid diagnosis date
    # Note: this table can only have one record per person           
    conditions = conditions.groupBy('person_id', 'covid_event_type').agg(F.min('condition_start_date').alias('first_diagnosis_date'))
    conditions = conditions.withColumn('diagnosis_type', F.when(F.col('covid_event_type') == 'Condition Diagnosis', F.lit(2)).otherwise(F.lit(3)))

 
    """
    1. Separate each indicator into different table each with different column names for indication date
    2. Outer join all indicator tables, 
    3. Add new column with the earliest indicator date 
    """
    # 1. separate indicator into new tables
    pcr_antigen = lab_measurements.select('person_id', F.col('first_diagnosis_date').alias('first_pos_pcr_antigen_date')).filter(F.col('diagnosis_type') == 1)
    antibody    = lab_measurements.select('person_id', F.col('first_diagnosis_date').alias('first_pos_antibody_date')).filter(F.col('diagnosis_type') == 4)
    diagnosis   = conditions.select('person_id', F.col('first_diagnosis_date').alias('first_pos_diagnosis_date')).filter(F.col('diagnosis_type') == 2)
    
    # 2. Outer join all
    all_pos_df = pcr_antigen.join(antibody, 'person_id', 'outer').join(diagnosis, 'person_id', 'outer')

    # 3. Create date columns with first indicator dates
    first_date_df = (
        all_pos_df
            .withColumn('first_poslab_or_diagnosis_date', 
                F.least('first_pos_pcr_antigen_date', 'first_pos_diagnosis_date'))     
            .withColumn('first_antigen_or_poslab_or_diagnosis_date', 
                F.least('first_pos_pcr_antigen_date', 'first_pos_antibody_date', 'first_pos_diagnosis_date'))                 
    )

    # New column identifies indicator associated with first_antigen_or_poslab_or_diagnosis_date
    event_type_df = (
        first_date_df
        .withColumn('covid_event_type', 
            F.when(F.col('first_antigen_or_poslab_or_diagnosis_date') == F.col('first_pos_pcr_antigen_date'),                    
            F.lit('PCR or Antigent Test'))
            .otherwise(
                F.when(F.col('first_antigen_or_poslab_or_diagnosis_date') == F.col('first_pos_diagnosis_date'),       
                F.lit('Condition Diagnosis'))
                .otherwise(
                    F.when(F.col('first_antigen_or_poslab_or_diagnosis_date') == F.col('first_pos_antibody_date'),    
                    F.lit('Antibody Test'))))
        )
    ).select('person_id', 
             'covid_event_type', 
             'first_pos_pcr_antigen_date', 
             'first_pos_diagnosis_date', 
             'first_pos_antibody_date', 
             'first_poslab_or_diagnosis_date',
             'first_antigen_or_poslab_or_diagnosis_date')

    # switches between returning only patients with positive PCR/Antigen tests
    # or PCR/Antigen tests, positive antibody tests, and positive diagnoses
    if USE_POSLAB_ONLY == True:
        df = (
            event_type_df
            .select('person_id', 'first_pos_pcr_antigen_date') 
            .filter(F.col('first_pos_pcr_antigen_date').isNotNull())       
        )
    else:
        df = event_type_df

    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0ae987d2-38b5-42db-b5c4-97b439fa504b"),
    ALL_COVID_POS_PATIENTS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    person_lds=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66")
)
"""
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:
Add in patient data for data exploration. 
This is a reference table and is not used downstream

Input:

Output:

"""
def ALL_COVID_POS_PERSONS(person_lds, ALL_COVID_POS_PATIENTS, location):

    acpp_with_persons_df = ALL_COVID_POS_PATIENTS.join(person_lds, 'person_id', "left")

    return acpp_with_persons_df.join(location.drop('data_partner_id'), 'location_id', "left")
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce"),
    covid_diag_recs=Input(rid="ri.foundry.main.dataset.32a1b5af-51ef-4b7b-acdd-344a94656be6"),
    covid_pos_recs=Input(rid="ri.foundry.main.dataset.7f6f5a78-e79a-4c4d-8549-75f919d84cdc")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:
Creates a table of all Covid+ patients with dates of *all* positive PCR or Antigen 
lab results.

By switching the USE_POSLAB_ONLY boolean value to False, patients with positive 
COVID diagnoses are included.

Input:

Output:
================================================================================
"""

def PATIENT_COVID_POS_DATES(covid_pos_recs, covid_diag_recs):

    #If set to True, then keeps only PCR /Antigen positive test as COVID_indicator
    USE_POSLAB_ONLY = True

    # reduce number of covid_pos_recs columns and create friendlier names for concept set names 
    # removes positive antibody tests    
    lab_measurements = (covid_pos_recs.select('person_id',F.col('measurement_date').alias('covid_date'), 'concept_set_name'))  
    lab_measurements = (
        lab_measurements
            .withColumn('covid_event_type', 
                F.when(F.col('concept_set_name') == 'ATLAS SARS-CoV-2 rt-PCR and AG', F.lit('PCR or Antigen Test'))
                    .otherwise(F.lit('Other lab measurement'))
            )
    ).where(F.col('covid_event_type') == 'PCR or Antigen Test')

    # reduce number of covid_diag_recs columns and create friendlier name for concept set name 
    conditions = covid_diag_recs.select('person_id', F.col('condition_start_date').alias('covid_date'), 'concept_set_name')
    conditions = (
        conditions
            .withColumn('covid_event_type', F.when(F.col('concept_set_name') == 'N3C Covid Diagnosis', F.lit('Condition Diagnosis'))
            .otherwise(F.lit('Other Condition'))
        )
    )  

    # join pos labs and diagnoses
    all_pos_df = lab_measurements.union(conditions).dropDuplicates()

    # switches between returning only patients with positive PCR/Antigen tests
    # or PCR/Antigen tests and positive diagnoses
    if USE_POSLAB_ONLY == True:
        df = (
            all_pos_df
            .filter(F.col('covid_event_type') == "PCR or Antigen Test")       
        )
    else:
        df = all_pos_df

    # creates one partition so the result table remains sorted
    df = df.coalesce(1)
    df = df.sort('person_id','covid_date')

    # New column for days_since_pos_pcr_antigen
    w = Window.partitionBy('person_id').orderBy('covid_date')
    df = (
        df
        .select('person_id','covid_date')
        .withColumn('last_covid_date', F.lag('covid_date').over(w))
        .withColumn('days_since_pos_pcr_antigen', F.datediff(F.col('covid_date'),F.col('last_covid_date')))
        .drop(F.col('last_covid_date'))            
    )

    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c946019f-1120-45c6-82d9-fdc431f7d83d"),
    antibody_concepts=Input(rid="ri.foundry.main.dataset.48d29d6b-0291-4ac3-94af-8c25a941348e"),
    pcr_ag_concepts=Input(rid="ri.foundry.main.dataset.ad9adb17-1e40-42da-8ee9-bf35afdd4486")
)
def all_tests_concepts(pcr_ag_concepts, antibody_concepts):

    return pcr_ag_concepts.union(antibody_concepts).distinct()

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.48d29d6b-0291-4ac3-94af-8c25a941348e"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
def antibody_concepts(concept_set_members):

    antibody_concept_set_name = "Atlas #818 [N3C] CovidAntibody retry"

    df = (
        concept_set_members
            .filter(concept_set_members.concept_set_name == antibody_concept_set_name)
            .filter(concept_set_members.is_most_recent_version == "true")
            .filter(concept_set_members.version.isNotNull())
    )    

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.113b0d2e-82c0-4a8c-b941-3c4232980169"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
def covid_diag_concepts(concept_set_members):

    covid_diag_concept_set_name = "N3C Covid Diagnosis"

    df = (
        concept_set_members
            .filter(concept_set_members.concept_set_name == covid_diag_concept_set_name)
            .filter(concept_set_members.is_most_recent_version == "true")
            .filter(concept_set_members.version.isNotNull()) 
    )

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.32a1b5af-51ef-4b7b-acdd-344a94656be6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    covid_diag_concepts=Input(rid="ri.foundry.main.dataset.113b0d2e-82c0-4a8c-b941-3c4232980169")
)
def covid_diag_recs(covid_diag_concepts, condition_occurrence):

    df = (
        condition_occurrence
            .join(covid_diag_concepts
                .select('concept_set_name', 'concept_id'),
                condition_occurrence.condition_concept_id == covid_diag_concepts.concept_id,
                "inner"
            )
    )

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7f6f5a78-e79a-4c4d-8549-75f919d84cdc"),
    covid_test_records=Input(rid="ri.foundry.main.dataset.5c9bc057-4190-472b-9520-924a679569fe"),
    resultpos_concepts=Input(rid="ri.foundry.main.dataset.f3312a49-7664-4ab5-9408-b143ab900570")
)
def covid_pos_recs(resultpos_concepts, covid_test_records):
 
    df = (
        covid_test_records
            .join(
                resultpos_concepts.select('concept_id'),
                covid_test_records.value_as_concept_id == resultpos_concepts.concept_id,
                "inner"
            ).drop(resultpos_concepts.concept_id)        
    )

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5c9bc057-4190-472b-9520-924a679569fe"),
    all_tests_concepts=Input(rid="ri.foundry.main.dataset.c946019f-1120-45c6-82d9-fdc431f7d83d"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
def covid_test_records(measurement, all_tests_concepts):

    df = (
        measurement
            .join(all_tests_concepts
                .select('concept_set_name', 'concept_id'),
                measurement.measurement_concept_id == all_tests_concepts.concept_id,
                "inner"
            )
    )
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ad9adb17-1e40-42da-8ee9-bf35afdd4486"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
def pcr_ag_concepts(concept_set_members):

    pcr_ag_concept_name = "ATLAS SARS-CoV-2 rt-PCR and AG"

    df = (
        concept_set_members
            .filter(concept_set_members.concept_set_name == pcr_ag_concept_name)
            .filter(concept_set_members.is_most_recent_version == "true")
            .filter(concept_set_members.version.isNotNull())
    ) 

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f3312a49-7664-4ab5-9408-b143ab900570"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
def resultpos_concepts(concept_set_members):

    resultpos_concept_set_name = "ResultPos"

    df = (
        concept_set_members
            .filter(concept_set_members.concept_set_name == resultpos_concept_set_name)
            .filter(concept_set_members.is_most_recent_version == "true")
            .filter(concept_set_members.version.isNotNull())
    )      
    
    return df

