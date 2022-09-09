from pyspark.sql import functions as F
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5e4e1582-5f65-4ffd-bab8-0f3e1785fc86"),
    zip_daily_obs=Input(rid="ri.foundry.main.dataset.f9140a24-d7b8-409a-8d4f-3ccd8532e54c")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-26

Description:
Renames preceding dataset

Input:
1.  zip_daily_obs
    All daily monitor observations
================================================================================
"""
def SO2_DAILY_OBS(zip_daily_obs):

    df = zip_daily_obs

    return zip_daily_obs    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1323a4e6-4ecf-4a72-9120-a6a6001dbce4"),
    ZiptoZcta_Crosswalk_2021_updated22=Input(rid="ri.foundry.main.dataset.4bb66747-0d03-4bb3-8a19-bccb89eac87d"),
    ZiptoZcta_Crosswalk_2021_ziptozcta2020=Input(rid="ri.foundry.main.dataset.99aaa287-8c52-4809-b448-6e46999a6aa7")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-22

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

    return ZiptoZcta_Crosswalk_2021_updated22.union(ZiptoZcta_Crosswalk_2021_ziptozcta2020)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.68346df0-d8c6-4cb4-91b6-6dd02b607e90"),
    so2_only=Input(rid="ri.foundry.main.dataset.e6fb2ce0-a437-4389-9952-3e7a096c9a6a")
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
SampleDuration

Input:
1.  so2_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    SO2 daily monitor measurements.
================================================================================
"""
def max_obs(so2_only):
 
    # There are negative and zero ArithmeticMean values. 
    # Negative values are set to Null and to 
    # insure they are not used in calculating averages. 
    so2_nullify_zeros_df =(
        so2_only
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
    w1 = Window.partitionBy('aqs_site_id','DateLocal','SampleDuration')
    maxObs_site_day_dur_df = (
        so2_nullify_zeros_df
        .select('aqs_site_id','DateLocal','SampleDuration','ObservationCount','ArithmeticMean')
        .withColumn("maxObservationCount", (F.max(F.col("ObservationCount")).over(w1)))
        #.filter(F.col('ObservationCount') == F.col('maxObservationCount')) 
    )  

    return maxObs_site_day_dur_df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d97e7753-5fd9-4241-98ba-b67772f597ff"),
    Daily_AirPollution_for_N3C=Input(rid="ri.foundry.main.dataset.2acf9cc2-855f-41fc-83e5-679382849ca9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-26

Description:
Gets average all of measurements from the same SampleDuration, same
ObservationCount, but different MethodCode. Measurements are only included 
from dates that have more than one measurement.          

Input:
1.  Daily_AirPollution_for_N3C 
    Daily air pollution measurements for many pollutants collected by EPA 
    monitors. Monitor locations are defined by Latitude and Longitude and
    ZCTAs. Patient ZIP Codes will be mapped to the monitor ZCTAs. 
================================================================================
"""
def methods_compare(Daily_AirPollution_for_N3C):

    '''
    ParameterCode : ParameterName
    42401   : Sulfer dioxide

    MethodCode : MethodName
    60      : INSTRUMENTAL - PULSED FLUORESCENT
    566     : INSTRUMENTAL - Pulsed Fluorescent 43C-TLE/43i-TLE
    600     : Instrumental - Ultraviolet Fluorescence API 100 EU
    '''
    PARAMETER_CODE  = 42401 
    SITE_ID         = "15-003-0010" 
    SAMPLE_DURATION = "1 HOUR"
    METHOD_CODE1    = 60
    METHOD_CODE2    = 566
    METHOD_CODE3    = 600    
    METHOD_CODE4    = None             

    daily_ap_df = (
        Daily_AirPollution_for_N3C
        .select('aqs_site_id'       ,
                'DateLocal'         ,
                'ParameterCode'     ,
                'ParameterName'     ,
                'ArithmeticMean'    ,
                'SampleDuration'    ,
                'MethodCode'        ,
                'MethodName'        ,
                'ObservationCount'  ,
                'UnitsofMeasure'
        )
        .filter(F.col('ParameterCode')  == PARAMETER_CODE   )         
        .filter(F.col('aqs_site_id')    == SITE_ID          )
        .filter(F.col('SampleDuration') == SAMPLE_DURATION  )                        
    )

    # There are negative and zero ArithmeticMean values. 
    # Negative values are set to Null and to 
    # insure they are not used in calculating averages. 
    nullify_neg_values_df =(
        daily_ap_df
        .withColumn('ArithmeticMean', 
                    F.when(F.col('ArithmeticMean') < 0, None)
                     .otherwise(F.col('ArithmeticMean'))
        )        
    )

    monitors_w_2_meas_day_df = (
        nullify_neg_values_df
        .groupBy('DateLocal')
        .count()
        .filter(F.col('count') > 1)
    )

    monitors_w_2_meas_day_all_columns_df = (
        nullify_neg_values_df
        .join(monitors_w_2_meas_day_df, 'DateLocal', 'inner')
    )

    # Only keeps daily records if observation counts are same fo all measurementz
    w1 = Window.partitionBy('DateLocal')
    sameObs_site_day_df = (
        monitors_w_2_meas_day_all_columns_df
        #.select('aqs_site_id','DateLocal','SampleDuration','ObservationCount','ArithmeticMean')
        .withColumn("maxObservationCount", (F.max(F.col("ObservationCount")).over(w1)))
        .withColumn("avgObservationCount", (F.avg(F.col("ObservationCount")).over(w1)))        
        .filter(F.col('avgObservationCount') == F.col('maxObservationCount')) 
    )    

    method_code1_avg = (
        sameObs_site_day_df
        .filter(F.col('MethodCode') == METHOD_CODE1)        
        .groupBy('MethodName', 'MethodCode')       
        .agg(F.avg('ArithmeticMean').alias('Measurement_Average'))
    )
        
    method_code2_avg = (
        sameObs_site_day_df
        .filter(F.col('MethodCode') == METHOD_CODE2)        
        .groupBy('MethodName', 'MethodCode')       
        .agg(F.avg('ArithmeticMean').alias('Measurement_Average'))
    )

    method_code3_avg = (
        sameObs_site_day_df
        .filter(F.col('MethodCode') == METHOD_CODE3)        
        .groupBy('MethodName', 'MethodCode')       
        .agg(F.avg('ArithmeticMean').alias('Measurement_Average'))
    )    

    df = method_code1_avg.union(method_code2_avg).union(method_code3_avg)

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.4d617b80-247d-44ca-9240-adeb278ae1a6"),
    so2_only=Input(rid="ri.foundry.main.dataset.e6fb2ce0-a437-4389-9952-3e7a096c9a6a")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-25

Description:
Displays a count value for the number of measurements from individual monitors 
collecrted on a single day. Used for error-checking.

Input:
1.  so2_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    SO2 daily monitor measurements.
================================================================================
"""
def monitor_per_date_count(so2_only):

    df = (
        so2_only
        .groupBy('aqs_site_id', 'DateLocal', 'SampleDuration')
        .count()
    ).filter(F.col('count') > 1)

    return df 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.cb2eea3a-ffdc-4aac-9f70-97c7c4774657"),
    pf_zip_zcta=Input(rid="ri.foundry.main.dataset.29aaf3f0-4b57-4d0e-841f-1529a5059433"),
    zcta_monitors_filter=Input(rid="ri.foundry.main.dataset.b8b5a54e-6e46-4d41-bd9d-00dfcde420bf")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-05

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
    Output(rid="ri.foundry.main.dataset.1ed9868e-78ed-462a-aa02-4c208be9fc8f"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-09

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
        # For testing: Fill in empty quotes with person_id from 
        # COVID_POS_PERSON_FACT with a zip_code = 10035
        df = COVID_POS_PERSON_FACT.filter(F.col('person_id') == '').select('zip_code').distinct()
    else:
        df = COVID_POS_PERSON_FACT.select('zip_code').distinct()

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.29aaf3f0-4b57-4d0e-841f-1529a5059433"),
    ZiptoZcta_Crosswalk=Input(rid="ri.foundry.main.dataset.1323a4e6-4ecf-4a72-9120-a6a6001dbce4"),
    pf_zip_code=Input(rid="ri.foundry.main.dataset.1ed9868e-78ed-462a-aa02-4c208be9fc8f")
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
    Output(rid="ri.foundry.main.dataset.8f588c0c-9721-46c9-bcd4-913848e0b1cd"),
    so2_only=Input(rid="ri.foundry.main.dataset.e6fb2ce0-a437-4389-9952-3e7a096c9a6a")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-26

Description:
Incoming data has one daily measurement per monitor per row. This process 
returns *all* daily measurements per monitor per day on one row. 

For SO2, a monitor may collect with the following sample durations:
"1 HOUR"
"3-HR BLK AVG"

How daily measurements are determined:
1. Getting daily measurements per SampleDuration: 
    - If there are multiple daily measurements from the same site, with 
      the same SampleDuration, the measurement from the record with the 
      highest ObservationCount is used.

    - If there are multiple daily measurements from the same site, with 
      the same SampleDuration, and the same maximum ObservationCount, the 
      measurements from those records are averaged. 

2. Choosing daily measurements between SampleDuration 
The final daily measurement for a monitor is chosen from the first, non-null
value from the sample durations in the following order:
1. 1 HOUR
2. 3-HR BLK AVG

Input:
1.  so2_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    SO2 daily monitor measurements.
================================================================================
"""
def so2_aggregations(so2_only):

    # There are negative and zero ArithmeticMean values. 
    # Negative values are set to Null and to 
    # insure they are not used in calculating averages. 
    so2_nullify_zeros_df =(
        so2_only
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
    w1 = Window.partitionBy('aqs_site_id','DateLocal','SampleDuration')
    maxObs_site_day_dur_df = (
        so2_nullify_zeros_df
        .select('aqs_site_id','DateLocal','SampleDuration','ObservationCount','ArithmeticMean')
        .withColumn("maxObservationCount", (F.max(F.col("ObservationCount")).over(w1)))
        .filter(F.col('ObservationCount') == F.col('maxObservationCount')) 
    )    

    # All "1 HOUR" monitor measurement per day. 
    # Multiple daily measurements are averaged.   
    monitor_01hr_df = (
        maxObs_site_day_dur_df
        .select('aqs_site_id', 'DateLocal','ArithmeticMean')
        .withColumnRenamed('aqs_site_id', 'aqs_site_id_01hr')
        .withColumnRenamed('DateLocal'  , 'DateLocal_01hr'  )           
        .filter(F.col('SampleDuration') == "1 HOUR")
        .groupBy('aqs_site_id_01hr','DateLocal_01hr')
        .agg(F.avg('ArithmeticMean').alias('measurement_dur01'))
    )
    

    # All "3-HR BLK AVG" monitor measurement per day. 
    # Multiple daily measurements are averaged.       
    monitor_03hrblk_df = (
        maxObs_site_day_dur_df
        .select('aqs_site_id', 'DateLocal', 'ArithmeticMean')
        .withColumnRenamed('aqs_site_id', 'aqs_site_id_03hrblk')
        .withColumnRenamed('DateLocal'  , 'DateLocal_03hrblk'  )                
        .filter(F.col('SampleDuration') == "3-HR BLK AVG")
        .groupBy('aqs_site_id_03hrblk','DateLocal_03hrblk')
        .agg(F.avg('ArithmeticMean').alias('measurement_dur03blk'))
    )

    # Merges all measurements on one row per measurement day.
    so2_merge_df = (
        monitor_01hr_df    
        .join(  monitor_03hrblk_df, 
                ( F.col('aqs_site_id_01hr') == F.col('aqs_site_id_03hrblk') )   &
                ( F.col('DateLocal_01hr'  ) == F.col('DateLocal_03hrblk'  ) )   ,
                'fullouter'                                                                               )
        .withColumn('aqs_site_id',  F.coalesce( F.col('aqs_site_id_01hr'), F.col('aqs_site_id_03hrblk') ) )
        .withColumn('date',         F.coalesce( F.col('DateLocal_01hr'  ), F.col('DateLocal_03hrblk'  ) ) )                  
    ).select('aqs_site_id', 'date', 'measurement_dur01', 'measurement_dur03blk')  

    # Generate dates for every day for each monitor's first day 
    # of measurement through its last day of measurement.
    all_days_df = (
        so2_merge_df
        .groupBy("aqs_site_id")
        .agg(F.min("date").alias("min_date"),
             F.max("date").alias("max_date"))
        .select("aqs_site_id", F.expr("sequence(min_date, max_date)").alias("date"))
        .withColumn("date", F.explode("date"))
        .withColumn("date", F.to_date("date", "yyyy-MM-dd"))        
    )

    # Join in rows for missing dates
    so2_all_days_df = all_days_df.join(so2_merge_df, ["aqs_site_id", "date"], "left")

    '''
    The final daily measurement for a monitor is chosen from the first, non-null
    value from the sample durations in the following order:
    1. 1 HOUR
    2. 3-HR BLK AVG
    '''
    so2_meas_df = (
        so2_all_days_df
        .withColumn('measurement_monitor_day', 
                    F.when(F.col('measurement_dur01'    ).isNotNull(),  F.col('measurement_dur01'   ))
                     .when(F.col('measurement_dur03blk' ).isNotNull(),  F.col('measurement_dur03blk'))
                     .otherwise(None)
        )       
    )

    # Create flags to indicate which sample duration measurement was used
    so2_measurement_flags_df = (
        so2_meas_df
        .withColumn('measurement_01hr',          
                    F.when(F.col('measurement_dur01').isNotNull()       ,   F.lit(1)).otherwise(F.lit(0)))
        .withColumn('measurement_03hrblk',
                    F.when(F.col('measurement_dur03blk').isNotNull()    &
                           F.col('measurement_dur01').isNull()          ,   F.lit(1)).otherwise(F.lit(0)))        
    )
    
    df = so2_measurement_flags_df

    return df         

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e6fb2ce0-a437-4389-9952-3e7a096c9a6a"),
    Daily_AirPollution_for_N3C=Input(rid="ri.foundry.main.dataset.2acf9cc2-855f-41fc-83e5-679382849ca9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-25

Description:
Subsets air pollution data to SO2 measurements with the following 
SampleDuration values:
"1 HOUR"
"3-HR BLK AVG"         

Input:
1.  Daily_AirPollution_for_N3C 
    Daily air pollution measurements for many pollutants collected by EPA 
    monitors. Monitor locations are defined by Latitude and Longitude and
    ZCTAs. Patient ZIP Codes will be mapped to the monitor ZCTAs. 
================================================================================
"""

def so2_only(Daily_AirPollution_for_N3C):

    # parameters
    parameter_code      = 42401
    sample_duration1    = "1 HOUR"
    sample_duration2    = "3-HR BLK AVG"        
    
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
        .filter( (F.col('SampleDuration') == sample_duration1) | (F.col('SampleDuration') == sample_duration2) )               
    )

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8b5a54e-6e46-4d41-bd9d-00dfcde420bf"),
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
def zcta_monitors_filter( ZCTA_Monitor_Pairs_Within_or_Within20km):
    
    # Lowercase / rename to match join in next transform
    zcta_monitor_df = (
        ZCTA_Monitor_Pairs_Within_or_Within20km
        .withColumnRenamed('ZCTA', 'zcta')
        .withColumnRenamed('Monitor_ID', 'aqs_site_id')
        .select('zcta', 'aqs_site_id', 'Distance_m', 'WithinZCTA')        
    ) 

    USE_SAMPLE = False

    if USE_SAMPLE == True:
        # filters to ZCTA in East Harlem which is near 20 monitors
        df = zcta_monitor_df.filter(F.col('zcta') == '10035')
    else:
        df = zcta_monitor_df

    return df    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a02d2a66-f6ee-4d42-a216-307d8af5934a"),
    monitors_nearby=Input(rid="ri.foundry.main.dataset.cb2eea3a-ffdc-4aac-9f70-97c7c4774657"),
    so2_aggregations=Input(rid="ri.foundry.main.dataset.8f588c0c-9721-46c9-bcd4-913848e0b1cd")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-26

Description:
Joins zip_code/zcta with all daily air pollution observations

Input:
1.  so2_aggregations
    All daily measurements per monitor

2.  monitors_nearby    
    nearby monitors to patient-based Zip Code / ZCTA
================================================================================
"""
def zip_all_monitor_obs(monitors_nearby, so2_aggregations):

    zip_measurement_df = (
            so2_aggregations
            .join(monitors_nearby, 'aqs_site_id', 'left')
            .filter(F.col('zip_code').isNotNull())
            .dropDuplicates()
    )

    return zip_measurement_df    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f9140a24-d7b8-409a-8d4f-3ccd8532e54c"),
    so2_only=Input(rid="ri.foundry.main.dataset.e6fb2ce0-a437-4389-9952-3e7a096c9a6a"),
    zip_all_monitor_obs=Input(rid="ri.foundry.main.dataset.a02d2a66-f6ee-4d42-a216-307d8af5934a")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-09

Description:
1. Rolls up observations by [zip_code, date] and produces:
    - the daily average of measurements from the monitors used
    - the total number of monitors used (with non-null measurements) to calculate
      the daily average 
    - the number of monitors used per SampleDuration:
        - 1 HR
        - 3 HR BLK AVG

2. Creates a count of non-null measurements and measurement averages for:
   - the  30 calendar days prior to current date 
   - the 365 calendar days prior to current date

Input:
1.  zip_all_monitor_obs
    zip_code/zcta with all daily air pollution observations
2.  so2_only
    SO2 pollutant data. Used to add ParameterName and UnitsofMeasure columns.
================================================================================
"""
def zip_daily_obs(zip_all_monitor_obs, so2_only):

    # Aggregate values by [zip_code, date]
    zamo_agg_df = (
        zip_all_monitor_obs
        .groupBy('zip_code', 'date')
        .agg(F.avg('measurement_monitor_day'    ).alias('measurement_avg')  ,
             F.sum('measurement_01hr'           ).alias('num_01hr_obs'   )  ,   
             F.sum('measurement_03hrblk'        ).alias('num_03hrblk_obs')  )
        .withColumn('num_monitors', F.col('num_01hr_obs') + F.col('num_03hrblk_obs'))          
        .select('zip_code'          , 
                'date'              ,                
                'measurement_avg'   , 
                'num_monitors'      , 
                'num_01hr_obs'      , 
                'num_03hrblk_obs'   
        )  
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

    # Get SO2 ParameterName, ParameterName, and UnitsofMeasure from first rows
    # of SO2 pollution data and join to result set  
    so2_name_units_df = so2_only.select('ParameterName', 'ParameterCode', 'UnitsofMeasure').limit(1)
    so2_df = zamo_prev_counts_df.join(so2_name_units_df)

    df = (
        so2_df
        .select('zip_code'                  , 
                'date'                      , 
                'ParameterName'             , 
                'ParameterCode'             ,
                'UnitsofMeasure'            ,
                'measurement_avg'           , 
                "num_monitors"              ,
                "num_01hr_obs"              ,
                "num_03hrblk_obs"           ,
                "prev_30_day_avg"           ,
                "prev_30_days_w_obs"        ,
                "prev_365_day_avg"          ,
                "prev_365_days_w_obs"       )
        .withColumnRenamed('date', 'meas_date')
    )  

    return df

    
        

