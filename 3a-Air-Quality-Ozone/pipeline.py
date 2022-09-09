from pyspark.sql import functions as F
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.20087501-90c0-4c8d-9690-a6785d78f35c"),
    zip_daily_obs=Input(rid="ri.foundry.main.dataset.6682899c-6339-4d46-ab67-99f28ab6868d")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-10

Description:
Renames preceding dataset

Input:
1.  zip_daily_obs
    All daily monitor observations
================================================================================
"""
def OZONE_DAILY_OBS(zip_daily_obs):
    
    df = zip_daily_obs

    return zip_daily_obs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.df949a46-fad5-4b24-ab0a-f38e7b158208"),
    ZiptoZcta_Crosswalk_2021_updated22=Input(rid="ri.foundry.main.dataset.4bb66747-0d03-4bb3-8a19-bccb89eac87d"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-08

Description:
Stacks ZiptoZcta_Crosswalk_2021_updated22 and ZiptoZcta_Crosswalk_2021_ziptozcta2020

Input:
1.  ZiptoZcta_Crosswalk_2021_ziptozcta2020
    Zip to ZCTA mappings
2.  ZiptoZcta_Crosswalk_2021_updated22
    Zip to ZCTA mappings update (contains only 22 records)

================================================================================
"""
def ZiptoZcta_Crosswalk(ZiptoZcta_Crosswalk_2021_updated22, ZiptoZcta_Crosswalk_2021_ziptozcta2020):

    current = ZiptoZcta_Crosswalk_2021_ziptozcta2020.select('ZIP_CODE', 'ZCTA')
    update  = ZiptoZcta_Crosswalk_2021_updated22.select('ZIP_CODE', 'ZCTA')

    df = current.union(update).distinct()
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e950c457-7678-46dd-bd7a-9b78422f5ad0"),
    ozone_only=Input(rid="ri.foundry.main.dataset.e76bb2b2-c9ad-426f-b99f-513eb90392a9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-26

Description:
Adds a column for max ObservationCount values for each group defined by the 
following columns. Error-checking so2_aggregations to make sure the correct 
record(s) are selected and aggregated.

Grouped By:
aqs_site_id
DateLocal

Input:
1.  ozone_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    ozone daily monitor measurements.
================================================================================
"""
def max_obs(ozone_only):

    ozone_nullify_bad_values_df =(
        ozone_only      
        .withColumn('ArithmeticMean', 
                    F.when(F.col('ArithmeticMean') < 0, None)
                     .otherwise(F.col('ArithmeticMean'))
        )
    )    

    # Create column with the largest ObservationCount for a monitor site
    # for each individual day, and only keep records from the same 
    # site-day which have the same max ObservationCount value. This will 
    # be used to identify sites that have more than one measurememts per 
    # site per day so these values can be averaged.
    w1 = Window.partitionBy('aqs_site_id','DateLocal')
    maxObs_per_site_day_df = (
        ozone_nullify_bad_values_df
        .select('aqs_site_id','DateLocal','ObservationCount','ArithmeticMean')
        .withColumn("maxObservationCount", (F.max(F.col("ObservationCount")).over(w1)))
        #.filter(F.col('ObservationCount') == F.col('maxObservationCount'))
    )    

    return maxObs_per_site_day_df    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.68110dcd-b624-48e2-b05d-fd4ff5f5c68e"),
    ozone_only=Input(rid="ri.foundry.main.dataset.e76bb2b2-c9ad-426f-b99f-513eb90392a9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-22

Description:
Displays a count value for the number of measurements from individual monitors 
collecrted on a single day. Used for error-checking.

Input:
1.  ozone_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    Ozone daily monitor measurements.
================================================================================
"""
def monitor_per_date_count(ozone_only):

    df = (
        ozone_only
        .groupBy('aqs_site_id', 'DateLocal')
        .count()
    ).filter(F.col('count') > 1)

    return df 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3a5ac6e4-474a-4535-b8ef-d594d5992ec1"),
    pf_zip_zcta=Input(rid="ri.foundry.main.dataset.13545c51-07d7-4c71-9ec7-474ec90afec6"),
    zcta_monitors_filter=Input(rid="ri.foundry.main.dataset.3dae3770-dd3d-4a26-aa18-6157dd107781")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-09

Description:
Joins nearby monitors to patient-based Zip Code / ZCTA

Input:
1.  zcta_monitors_filter
    ZCTAs within 20 km from monitor location (Lat/Long)

2.  pf_zip_zcta  
    ZCTAs and associated patient zip (according to ZCTA to Zip Crosswalk)  
================================================================================
"""
def monitors_nearby(zcta_monitors_filter, pf_zip_zcta):

    NEAREST_MONITOR_ONLY = False

    cohort_zip_zcta_distinct_df = pf_zip_zcta.select('zip_code','zcta').dropDuplicates()

    monitors_near_zip_df        = cohort_zip_zcta_distinct_df.join(zcta_monitors_filter, 'zcta', 'inner').dropDuplicates()

    # if NEAREST_MONITOR_ONLY == True:
    #     val w = Window.partitionBy($"id")
    #     val df2 = df.withColumn("maxCharge", max("charge").over(w))
    #     .filter($"maxCharge" === $"charge")
    #     .drop("charge")
    #     .withColumnRenamed("maxCharge", "charge")        
    #     return monitors_near_zip_df.
    
    return monitors_near_zip_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d5a62ea1-186b-421d-a73b-0fb0157287dc"),
    ozone_only=Input(rid="ri.foundry.main.dataset.e76bb2b2-c9ad-426f-b99f-513eb90392a9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-25

Description:
Incoming data has one daily measurement per monitor per row. This process 
returns one record for each monitor for each day. 

How daily measurements are determined:
 - If there are multiple measurements from the same site, the measurement 
   from the record with the highest ObservationCount is used.

 - If there are multiple measurements from the same site with the same maximum 
   ObservationCount, the measurements from those records are averaged. 

Input:
1.  ozone_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    Ozone daily monitor measurements.
================================================================================
"""
def ozone_aggregations(ozone_only):

    # All negative values are set to null to 
    # insure they are not used in calculating averages. 
    ozone_nullify_bad_values_df =(
        ozone_only      
        .withColumn('ArithmeticMean', 
                    F.when(F.col('ArithmeticMean') < 0, None)
                     .otherwise(F.col('ArithmeticMean'))
        )
    )    

    # Create column with the largest ObservationCount for a monitor site
    # for each individual day, and only keep records from the same 
    # site-day which have the same max ObservationCount value. This will 
    # be used to identify sites that have more than one measurememts per 
    # site per day so these values can be averaged.
    w1 = Window.partitionBy('aqs_site_id','DateLocal')
    maxObs_per_site_day_df = (
        ozone_nullify_bad_values_df
        .select('aqs_site_id','DateLocal','ObservationCount','ArithmeticMean')
        .withColumn("maxObservationCount", (F.max(F.col("ObservationCount")).over(w1)))
        .filter(F.col('ObservationCount') == F.col('maxObservationCount'))
    )    

    # Get average of the ArithmeticMean from sites with multiple
    # measurements on the same day that have the same max 
    # ObservationCount value  
    ozone_meas_avg_df = (
        maxObs_per_site_day_df
        .groupBy('aqs_site_id','DateLocal')
        .agg(F.avg('ArithmeticMean').alias('measurement'))
    )

    # Generate dates for every day for each monitor's first day 
    # of measurement through its last day of measurement.
    all_days_df = (
        ozone_meas_avg_df
        .groupBy("aqs_site_id")
        .agg(F.min("DateLocal").alias("min_date"),
             F.max("DateLocal").alias("max_date"))
        .select("aqs_site_id", F.expr("sequence(min_date, max_date)").alias("DateLocal"))
        .withColumn("DateLocal", F.explode("DateLocal"))
        .withColumn("DateLocal", F.to_date("DateLocal", "yyyy-MM-dd"))        
    )

    # Join in rows for missing dates
    ozone_all_days_df = all_days_df.join(ozone_meas_avg_df, ["aqs_site_id", "DateLocal"], "left")

    df = ozone_all_days_df.withColumnRenamed('DateLocal','date') 

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e76bb2b2-c9ad-426f-b99f-513eb90392a9"),
    Daily_AirPollution_for_N3C=Input(rid="ri.foundry.main.dataset.2acf9cc2-855f-41fc-83e5-679382849ca9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-22

Description:
Subsets air pollution data to Ozone measurements and 
SampleDuration == "8-HR RUN AVG BEGIN HOUR"     

Input:
1.  Daily_AirPollution_for_N3C 
    Daily air pollution measurements for many pollutants collected by EPA 
    monitors. Monitor locations are defined by Latitude and Longitude and
    ZCTAs. Patient ZIP Codes will be mapped to the monitor ZCTAs. 
================================================================================
"""

def ozone_only(Daily_AirPollution_for_N3C):

    # parameters
    parameter_code  = 44201
    sample_duration = "8-HR RUN AVG BEGIN HOUR"
    
    df = (
        Daily_AirPollution_for_N3C
        .select('aqs_site_id'       ,
                'DateLocal'         ,
                'ArithmeticMean'    ,
                'UnitsofMeasure'    ,
                'SampleDuration'    ,
                'ObservationCount'  ,
                'ParameterCode'     ,
                'ParameterName'     ,
                'CityName'          ,
                'StateName'         , 
                'Latitude'          , 
                'Longitude'
        )
        .filter(F.col('ParameterCode')  == parameter_code)
        .filter(F.col('SampleDuration') == sample_duration)
    ) 

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.201b2820-791f-455b-94f4-084a547f0414"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-05

Description:
Gets distint ZIP Codes from Covid+ patients. 

To get a subset of ZIP Codes, set USE_SAMPLE = True
For all patient ZIP Codes,    set USE_SAMPLE = False

Input:
1.  COVID_POS_PERSON_FACT
    All Covid+ patients and associated atomic data
================================================================================
"""
def pf_zip_code(COVID_POS_PERSON_FACT):

    USE_SAMPLE = False

    if USE_SAMPLE == True:
        # returns one person from zip_code = 10035, born in 1935, has covid-associated hospitalization, etc., 
        df = COVID_POS_PERSON_FACT.filter(F.col('person_id') == '2995272647743632189').select('zip_code').distinct()
    else:
        df = COVID_POS_PERSON_FACT.select('zip_code').distinct()

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.13545c51-07d7-4c71-9ec7-474ec90afec6"),
    ZiptoZcta_Crosswalk=Input(rid="ri.foundry.main.dataset.df949a46-fad5-4b24-ab0a-f38e7b158208"),
    pf_zip_code=Input(rid="ri.foundry.main.dataset.201b2820-791f-455b-94f4-084a547f0414")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-05

Description:
Adds associated ZCTAs for patients based on their Zip Code

Input:
1.  pf_zip_code
    Patient Zip Codes

2.  ZiptoZcta_Crosswalk  
    Maps ZIP Codes to ZTCAs  
================================================================================
"""
def pf_zip_zcta(pf_zip_code, ZiptoZcta_Crosswalk):
    
    # lower case column names
    zip_to_zcta_df = ZiptoZcta_Crosswalk.select(F.col('ZIP_CODE').alias('zip_code'), F.col('ZCTA').alias('zcta'))

    # join zcta to zip_code
    df = pf_zip_code.join(zip_to_zcta_df, 'zip_code', 'left').select('zip_code', 'zcta')

    return df 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3dae3770-dd3d-4a26-aa18-6157dd107781"),
    ZCTA_Monitor_Pairs_Within_or_Within20km=Input(rid="ri.foundry.main.dataset.198610c4-b080-4c4c-b315-416055315094")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-09

Description:
ZCTAs within 20 km from monitor location (Lat/Long).

To get a subset of ZCTAs, set USE_SAMPLE = True
To get all ZCTAs,         set USE_SAMPLE = False

Input:
1.  ZCTA_Monitor_Pairs_Within_or_Within20km
    ZCTAs within 20 km from monitor location (Lat/Long)
================================================================================
"""
def zcta_monitors_filter(ZCTA_Monitor_Pairs_Within_or_Within20km):
    
    # Lowercase / rename to match join in next transform
    zcta_monitor_df = (
        ZCTA_Monitor_Pairs_Within_or_Within20km
        .withColumnRenamed('ZCTA', 'zcta')
        .withColumnRenamed('Monitor_ID', 'aqs_site_id')
        .select('zcta', 'aqs_site_id', 'Distance_m', 'WithinZCTA')        
    ) 

    USE_SAMPLE = False

    if USE_SAMPLE == True:
        # Currently filters to ZCTA in East Harlem which is near 20 monitors
        df = zcta_monitor_df.filter(F.col('zcta') == '10035')
    else:
        df = zcta_monitor_df

    return df    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4ba4059d-4d4c-4687-9582-b7145d195cb9"),
    monitors_nearby=Input(rid="ri.foundry.main.dataset.3a5ac6e4-474a-4535-b8ef-d594d5992ec1"),
    ozone_aggregations=Input(rid="ri.foundry.main.dataset.d5a62ea1-186b-421d-a73b-0fb0157287dc")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-07

Description:
Joins zip_code/zcta with all daily air pollution observations

Input:
1.  ozone_aggregations
    All daily measurements per monitor

2.  monitors_nearby    
    nearby monitors to patient-based Zip Code / ZCTA
================================================================================
"""
def zip_all_monitor_obs(monitors_nearby, ozone_aggregations):

    zip_measurement_df = (
            ozone_aggregations
            .join(monitors_nearby, 'aqs_site_id', 'left')
            .filter(F.col('zip_code').isNotNull())
            .dropDuplicates()
    )

    return zip_measurement_df
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6682899c-6339-4d46-ab67-99f28ab6868d"),
    ozone_only=Input(rid="ri.foundry.main.dataset.e76bb2b2-c9ad-426f-b99f-513eb90392a9"),
    zip_all_monitor_obs=Input(rid="ri.foundry.main.dataset.4ba4059d-4d4c-4687-9582-b7145d195cb9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-09

Description:
1. Rolls up observation by [zip_code, date] and produces:
   - the number of monitors with observations per day
   - the average of all monitors per day

2. Creates a count of non-null measurements and measurement averages for:
   - the  30 calendar days prior to current date 
   - the 365 calendar days prior to current date

Input:
1.  zip_all_monitor_obs
    zip_code/zcta with all daily air pollution observations
2.  ozone_only
    Ozone pollutant data. Used to add ParameterName and UnitsofMeasure columns.
================================================================================
"""
def zip_daily_obs(zip_all_monitor_obs, ozone_only):

    # Aggregate values by [zip_code, date]
    zamo_agg_df = (
        zip_all_monitor_obs
        .groupBy('zip_code', 'date')
        .agg(F.avg('measurement'  ).alias('measurement_avg')  ,
             F.count('measurement').alias('num_monitors'   )  )
        .select('zip_code'          , 
                'date'              ,                
                'measurement_avg'   , 
                'num_monitors'      )  
    )

    # function used in following Window funtions to count seconds in a day
    days = lambda i: i * 86400 

    # Creates window by casting timestamp to long (number of seconds) for previous 30 days
    w30 = (
        Window
        .partitionBy('zip_code')
        .orderBy(F.col("date").cast("timestamp").cast('long'))
        .rangeBetween(-days(30), -1)
    )

    # Rolling averages and counts of measurements for previous 30 days
    zamo_prev_avg_df = (
        zamo_agg_df
        .withColumn('prev_30_day_avg',      F.avg("measurement_avg"  ).over(w30) )    
        .withColumn('prev_30_days_w_obs',   F.count("measurement_avg").over(w30) )    
    )

    # Creates window by casting timestamp to long (number of seconds) for previous 365 days
    w365 = (
        Window
        .partitionBy('zip_code')
        .orderBy(F.col("date").cast("timestamp").cast('long'))
        .rangeBetween(-days(365), -1)
    )

    # Rolling averages and counts of measurements for previous 365 days
    zamo_prev_counts_df = (
        zamo_prev_avg_df
        .withColumn('prev_365_day_avg',      F.avg("measurement_avg"  ).over(w365) )    
        .withColumn('prev_365_days_w_obs',   F.count("measurement_avg").over(w365) )    
    )

    # Get Ozone ParameterName, ParameterCode, and UnitsofMeasure from first rows
    # of Ozone pollution data and join to result set  
    ozone_name_units_df = ozone_only.select('ParameterName', 'ParameterCode', 'UnitsofMeasure').limit(1)
    ozone_df = zamo_prev_counts_df.join(ozone_name_units_df)

    df = (
        ozone_df
        .select('zip_code'                  , 
                'date'                      , 
                'ParameterName'             ,
                'ParameterCode'             ,                 
                'UnitsofMeasure'            ,
                'measurement_avg'           , 
                "num_monitors"              ,
                "prev_30_day_avg"           ,
                "prev_30_days_w_obs"        ,
                "prev_365_day_avg"          ,
                "prev_365_days_w_obs"       )
        .withColumnRenamed('date', 'meas_date')
    )  

    return df

    

