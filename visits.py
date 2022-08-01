from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.55e4b029-354e-460c-b1bc-3cfe967feb66"),
    cohort_hosp_covid_flag=Input(rid="ri.foundry.main.dataset.88053ad8-3c3e-40fd-a78c-2d2ec2d11e7f")
)
def COHORT_HOSPITALIZATIONS(cohort_hosp_covid_flag):

    df = cohort_hosp_covid_flag

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.67a26ccb-3e27-484d-ab8e-ed4d145c3f33"),
    cohort_non_hosp_visits=Input(rid="ri.foundry.main.dataset.d1e4028f-9ad8-4a85-b834-74bd39154cf6")
)
def COHORT_VISITS(cohort_non_hosp_visits):
    
    df = cohort_non_hosp_visits

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.36dbee19-eb47-4532-b583-e0d20ce51c65"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    pf_filter=Input(rid="ri.foundry.main.dataset.19c3f7cf-f5cf-4a5b-bc97-92fadfb73cdc")
)
"""
Subset microvisit_to_macrovisit_lds to contain only cohort visits
Note: There appears to be some cohort patients not in the microvisit_to_macrovisit_lds table
"""
def cohort_all_visits( pf_filter, microvisit_to_macrovisit_lds):

    distinct_patients_df = pf_filter.select('person_id').dropDuplicates()
    
    visits_df = microvisit_to_macrovisit_lds.join(distinct_patients_df, 'person_id', 'inner')

    return visits_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d8e2b99b-b669-4e07-a9e2-7a999cee08d6"),
    cohort_all_visits=Input(rid="ri.foundry.main.dataset.36dbee19-eb47-4532-b583-e0d20ce51c65")
)
'''
Subset Cohort Visits to Cohort Hospitalizations (macrovisit_id != null)

'''
def cohort_hosp(cohort_all_visits):

    cohort_hosp_df = (
        cohort_all_visits
        .select('person_id','data_partner_id', 'macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
    ).dropDuplicates()

    return cohort_hosp_df 
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.88053ad8-3c3e-40fd-a78c-2d2ec2d11e7f"),
    cohort_hosp=Input(rid="ri.foundry.main.dataset.d8e2b99b-b669-4e07-a9e2-7a999cee08d6"),
    pf_filter=Input(rid="ri.foundry.main.dataset.19c3f7cf-f5cf-4a5b-bc97-92fadfb73cdc")
)
'''
Cohort hospitalizations 
Only uses positive PCR or Antigen Test as determiniation of COVID+
'''
def cohort_hosp_covid_flag(pf_filter, cohort_hosp):

    '''
    Hospitalization start date  (macrovisit_start_date) must start:
        num_days_after positive poslab_date
        and    
        num_days_before positive poslab_date

    example:
    If poslab_date        = June 10
    macrovisit_start_date = [June 9, June 26]     
    ''' 
    num_days_before             = 1
    num_days_after              = 16

    # only need these columns
    p1_poslab = (
        pf_filter
        .select('person_id', F.col('covid_date').alias('poslab_date'), 'covid_event_type')
        .where(F.col('covid_event_type') == "PCR or Antigen Test")    
    )

    # COVID-associated hospitalizations
    all_poslab_hosp_df = (
        cohort_hosp
        .join(p1_poslab, 'person_id', 'left')
        .where( (F.col('macrovisit_start_date') >= F.col('poslab_date') - num_days_before ) & 
                (F.col('macrovisit_start_date') <= F.col('poslab_date') + num_days_after  ) &
                (F.col('poslab_date')           <= F.col('macrovisit_end_date')           ) )
    )

    # Order by macrovisit_id, poslab_date
    w = Window.partitionBy('macrovisit_id').orderBy('poslab_date')

    # Keep only the first poslab_date for each hospitalization period.
    # Drop duplicates to get rid of instances when more than one  
    # positive lab results occur on the first poslab_date. 
    first_poslab_per_hosp_df = (
        all_poslab_hosp_df
        .withColumn('poslab_date', F.first('poslab_date').over(w))
        .select('macrovisit_id', 'poslab_date')
        .dropDuplicates()
        #.withColumn('diff',  F.datediff(F.col('macrovisit_start_date'), F.col('poslab_date')) )
    )

    # All hospitalizations (including COVID-associated)
    # Creates flags for hospitalization visista and covid-associated hospitalizations
    all_hosp_df = (
        cohort_hosp
        .join(first_poslab_per_hosp_df, 'macrovisit_id', 'left')
        .withColumn('covid_associated_hosp' , F.when(F.col('poslab_date').isNotNull(), F.lit(1) ).otherwise(0) )
        .select('person_id', 'macrovisit_id', 'covid_associated_hosp', 'poslab_date', 'macrovisit_start_date', 'macrovisit_end_date', 'data_partner_id')
    )

    return all_hosp_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d1e4028f-9ad8-4a85-b834-74bd39154cf6"),
    cohort_all_visits=Input(rid="ri.foundry.main.dataset.36dbee19-eb47-4532-b583-e0d20ce51c65")
)
'''
Keeps only cohort non-hospitalization visits 
'''
def cohort_non_hosp_visits(cohort_all_visits):

    df = (
        cohort_all_visits
        .filter(F.col('macrovisit_id').isNull())
        .drop('macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
    )
    
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.19c3f7cf-f5cf-4a5b-bc97-92fadfb73cdc"),
    PATIENT_COVID_POS_DATES=Input(rid="ri.foundry.main.dataset.9511c5d1-dcdd-4bb7-a73a-c880650111ce")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:
Filters out COVID diagnoses (Only positive PCR/AG lab tests are kept.)

Input:

Output:
================================================================================
"""

'''

'''
def pf_filter(PATIENT_COVID_POS_DATES):

    pf_poslab_only_df = ( 
        PATIENT_COVID_POS_DATES
        .filter(F.col('covid_event_type') == "PCR or Antigen Test")
    )
    
    
    return pf_poslab_only_df
    

