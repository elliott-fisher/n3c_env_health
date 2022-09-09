from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.590b3e52-6ac4-48dc-8af0-ae77a9d37eff"),
    climate_diag_date=Input(rid="ri.foundry.main.dataset.9dcffed1-c3b3-4b27-9691-c6365e2be9fc")
)
def CLIMATE_LAG_DIAGNOSIS_DATE(climate_diag_date):

    df = climate_diag_date
    
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9dcffed1-c3b3-4b27-9691-c6365e2be9fc"),
    person_zip_distinct_with_lags=Input(rid="ri.foundry.main.dataset.ddea251c-2c1c-4ecf-a7a6-4771bb4163da"),
    prism_with_distinct_zip=Input(rid="ri.foundry.main.dataset.59ea0ef7-d3c7-4daf-96e7-4be23f49f825")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:
Set of all patients and climate date for 45 days before and after the index date.

Input:

Output:
================================================================================
"""
def climate_diag_date(person_zip_distinct_with_lags, prism_with_distinct_zip):

    pers_df     = person_zip_distinct_with_lags
    clime_df    = prism_with_distinct_zip

    """
    Joins 
    """
    person_climate_df = (
        pers_df
            .join(clime_df, (pers_df.zip_code == clime_df.POSTCODE) & (pers_df.lag_dates == clime_df.Date) , 'left')
            .withColumn('day_of_week_num',  F.dayofweek('lag_dates')) 
            .withColumn('day_of_week',      F.date_format('lag_dates', 'E')) 
            .select('person_id'                     , 
                    'zip_code'                      , 
                    'first_pos_pcr_antigen_date'    , 
                    'time_lag'                      , 
                    'lag_dates'                     , 
                    'day_of_week_num'               , 
                    'day_of_week'                   , 
                    'Date'                          , 
                    'MeanTemp'                      , 
                    'Precip'                        , 
                    'DewPoint'                      )
            .withColumnRenamed('Date', 'climate_measure_date')                
    )
    
    return person_climate_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.98f40407-9dd1-4d28-bd50-8fb953ad2eb5"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810"),
    prism_2019_2020=Input(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:
Set of COVID+ patients who have zip_code values that match the POSTCODE values 
in the Prism data. 

Input:

Output:
================================================================================
"""
def person_zip_distinct(prism_2019_2020, COVID_POS_PERSON_FACT):
    covid_pos_person_fact = COVID_POS_PERSON_FACT
    
    clime_df    = prism_2019_2020
    patients_df = covid_pos_person_fact

    person_zip_covid_date = (
        patients_df
            .select('person_id', 'zip_code')
            .join(clime_df.select(F.col('POSTCODE').alias('zip_code')), 'zip_code', 'inner')
            .where(patients_df.zip_code.isNotNull())
            .distinct()
            .join(patients_df.select('person_id', 'first_pos_pcr_antigen_date'), 'person_id', 'left')
            .select('person_id', 'zip_code', 'first_pos_pcr_antigen_date')   
    )

    return person_zip_covid_date

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ddea251c-2c1c-4ecf-a7a6-4771bb4163da"),
    person_zip_distinct=Input(rid="ri.foundry.main.dataset.98f40407-9dd1-4d28-bd50-8fb953ad2eb5"),
    time_lags_45=Input(rid="ri.foundry.main.dataset.d27a09af-9d96-4ea3-ab38-9642acda388f")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:
Set of COVID+ patients who have zip_code values that match the POSTCODE values 
in the Prism data expanded to include records with lag dates of 45 days before 
and after the index date plus a record for the index date. This means there are 
91 records per patient.

Input:

Output:
================================================================================
"""
def person_zip_distinct_with_lags(person_zip_distinct, time_lags_45):

    with_lags_df = person_zip_distinct.crossJoin(time_lags_45)

    with_dates_df = with_lags_df.withColumn('lag_dates', with_lags_df.first_pos_pcr_antigen_date + with_lags_df.time_lag)

    return with_dates_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.71824b35-03e4-45f5-8741-a5cd607281ae"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810")
)
def pf_distinct_zip(COVID_POS_PERSON_FACT):
    
    distinct_zip = COVID_POS_PERSON_FACT.select('zip_code').distinct()

    return distinct_zip

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9"),
    ds_2019_EQI_Prism=Input(rid="ri.foundry.main.dataset.91f3e015-aa25-4f68-a310-a2c1e7a92d79"),
    ds_2020_EQI_Prism=Input(rid="ri.foundry.main.dataset.7a70bef9-0a83-48ba-9014-7af6df87e617")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:

Input:

Output:
================================================================================
"""
def prism_2019_2020(ds_2020_EQI_Prism, ds_2019_EQI_Prism):

    df = ds_2019_EQI_Prism.union(ds_2020_EQI_Prism)

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.102f8344-4581-4622-abf1-1d38d2e250d6"),
    prism_2019_2020=Input(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9")
)
def prism_distinct_postcodes(prism_2019_2020):

    distinct_postcodes = prism_2019_2020.select('POSTCODE').distinct()

    return distinct_postcodes
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.59ea0ef7-d3c7-4daf-96e7-4be23f49f825"),
    prism_2019_2020=Input(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9"),
    zip_distinct=Input(rid="ri.foundry.main.dataset.8c6e49c8-3cbd-4925-bfd0-d3c42bf01447")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:
The PRISM data subsetted to include records that have POSTCODE values that
are in the Patient data.

Input:

Output:
================================================================================
"""
def prism_with_distinct_zip(prism_2019_2020, zip_distinct):

    distinct_POSTCODE_list = list(zip_distinct.select('POSTCODE').toPandas()['POSTCODE'])

    df = (
        prism_2019_2020
            .select('POSTCODE', 'MeanTemp', 'Precip', 'DewPoint', 'Date')
            .where(F.col('POSTCODE').isin(distinct_POSTCODE_list))
            .orderBy(F.asc("Date"))
    )
        
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d27a09af-9d96-4ea3-ab38-9642acda388f")
)
def time_lags_45():

    # Create a DataFrame with single pyspark.sql.types.LongType column named "id", 
    # containing elements in a range from start to end (exclusive) with step value step. 
    # I changed the default column "id" to time_lag and cast from LongType to IntegerType
    df = spark.range(-45,46).select(F.col("id").alias('time_lag').cast("int"))

    return df.orderBy('time_lag')    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.df7414bd-1024-4a47-85d5-249ee7835a77"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810"),
    prism_2019_2020=Input(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9")
)
def unnamed_2(prism_2019_2020, COVID_POS_PERSON_FACT):
    
    prism_zip_df    = prism_2019_2020.select(F.col('POSTCODE').alias('zip_code')).distinct()

    pf_zip_df       = COVID_POS_PERSON_FACT.select('zip_code').distinct()

    pf_zips_not_in_prism = (
        prism_zip_df
        .join(pf_zip_df,'zip_code', 'right')
        .filter(prism_zip_df.zip_code.isNull())
    )
    
    w = Window().orderBy('zip_code')
    df = pf_zips_not_in_prism.withColumn("row_num", F.row_number().over(w))

    df = df.filter(F.col('row_num') >= 2000)

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8c6e49c8-3cbd-4925-bfd0-d3c42bf01447"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810"),
    prism_2019_2020=Input(rid="ri.foundry.main.dataset.f62fcd9f-db46-47b8-b510-7c9048bbf7a9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-20

Description:
Creates a table with all zip_code from the patient table that are 
also in the PRISM data

Input:

Output:
================================================================================
"""
def zip_distinct(prism_2019_2020, COVID_POS_PERSON_FACT):
    covid_pos_person_fact = COVID_POS_PERSON_FACT

    clime_df    = prism_2019_2020
    patients_df = covid_pos_person_fact

    zip_distinct_df = (
        patients_df
            .select(F.col('zip_code').alias('POSTCODE'))
            .join(clime_df.select('POSTCODE'), 'POSTCODE', 'inner') 
            .distinct()  
    )

    return zip_distinct_df    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.95d1232c-57a4-48cf-a230-e0ffed0c3e79"),
    pf_distinct_zip=Input(rid="ri.foundry.main.dataset.71824b35-03e4-45f5-8741-a5cd607281ae"),
    prism_distinct_postcodes=Input(rid="ri.foundry.main.dataset.102f8344-4581-4622-abf1-1d38d2e250d6")
)
def zip_not_in_prism(prism_distinct_postcodes, pf_distinct_zip):

    prism_distinct_zip_df   = prism_distinct_postcodes.withColumnRenamed('POSTCODE', 'zip_code')
    
    zip_not_in_prism_df     = (
        pf_distinct_zip
        .join(prism_distinct_zip_df, 'zip_code', 'left')
        .where(prism_distinct_zip_df.zip_code.isNull())
    )
    
    w = Window().orderBy(F.col('zip_code'))
    df = zip_not_in_prism_df.withColumn("row_num", F.row_number().over(w))

    return df.where(F.col('row_num') > 3000)     
    

