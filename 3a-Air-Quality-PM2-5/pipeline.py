from pyspark.sql import functions as F
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0937fbc2-c1f8-425d-98aa-884aa47f6492"),
    zip_daily_obs=Input(rid="ri.foundry.main.dataset.a3cef816-9f77-45ff-b10d-932c73bdc497")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-08

Description:
Renames preceding dataset

Input:
1.  zip_daily_obs
    All daily monitor observations
================================================================================
"""
def PM25_DAILY_OBS(zip_daily_obs):
    
    df = zip_daily_obs

    return zip_daily_obs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c9a82785-ab38-46ce-a391-4c64897d1ab1"),
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
    Output(rid="ri.foundry.main.dataset.d7b6ccfc-c58a-4da7-8df6-61f554220cfc"),
    pf_zip_zcta=Input(rid="ri.foundry.main.dataset.a097770a-3f86-4a5e-9171-aa185b0ad50a"),
    zcta_monitors_filter=Input(rid="ri.foundry.main.dataset.efe34955-db34-4705-b17c-e4410ff7aa1a")
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

    monitors_near_zip_df        = cohort_zip_zcta_distinct_df.join(zcta_monitors_filter, 'zcta', 'inner')

    # if NEAREST_MONITOR_ONLY == True:
    #     val w = Window.partitionBy($"id")
    #     val df2 = df.withColumn("maxCharge", max("charge").over(w))
    #     .filter($"maxCharge" === $"charge")
    #     .drop("charge")
    #     .withColumnRenamed("maxCharge", "charge")        
    #     return monitors_near_zip_df.
    
    return monitors_near_zip_df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a66c9f77-bb1f-4943-b0b6-338797130827"),
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
    Output(rid="ri.foundry.main.dataset.a097770a-3f86-4a5e-9171-aa185b0ad50a"),
    ZiptoZcta_Crosswalk=Input(rid="ri.foundry.main.dataset.c9a82785-ab38-46ce-a391-4c64897d1ab1"),
    pf_zip_code=Input(rid="ri.foundry.main.dataset.a66c9f77-bb1f-4943-b0b6-338797130827")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-09-09

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
    Output(rid="ri.foundry.main.dataset.6504028c-c02d-4696-8d6f-0c9104f4798f"),
    pm25_only=Input(rid="ri.foundry.main.dataset.f8ff70bf-e5e4-4113-8512-37b8b1e97738")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-06

Description:
Incoming data has one daily measurement per monitor per row. This process puts 
*all* daily measurements per monitor on one row. 

For PM 2.5, a monitor may collect for the following sample durations:
"24 HOUR"
"24-HR BLK AVG"
"1 HOUR" (only values with a ObservationCount = 24 are used)

If there are multiple measurements of the same sample duration collected at 
the same monitor on the same day, these measurements are averaged. For example,
if a monitor collects two 24 HOUR measurements on the same day, the average is
stored as the measurement for that day.

The final daily measurement for a monitor is chosen from the first, non-null
value from the sample durations in the following order:
1. 24 HOUR
2. 24-HR BLK AVG
3. 1 HOUR

Input:
1.  pm25_only:
    A dataset derived from Daily_AirPollution_for_N3C which contains only 
    PM 2.5 daily monitor measurements.
================================================================================
"""
def pm25_aggregations(pm25_only):

    # There are negative and zero ArithmeticMean values. 
    # Negative values are set to Null and to 
    # insure they are not used in calculating averages. 
    # Uncomment section to set values between [0-0.00001] are set to Zero    
    pm25_nullify_zeros_df =(
        pm25_only
        .withColumn('ArithmeticMean', 
                    F.when(F.col('ArithmeticMean') < 0, None)
                     .otherwise(F.col('ArithmeticMean'))
        )
    #    .withColumn('ArithmeticMean',
    #                F.when(F.col('ArithmeticMean').between(0, 0.00001), 0)                    
    #                 .otherwise(F.col('ArithmeticMean'))
    #    )         
    )

    # All "24 Hour" monitor measurement per day. 
    # Multiple daily measurements are averaged.   
    monitor_24hr_df = (
        pm25_nullify_zeros_df
        .select('aqs_site_id', 'DateLocal','ArithmeticMean')
        .withColumnRenamed('aqs_site_id', 'aqs_site_id_24hr')
        .withColumnRenamed('DateLocal'  , 'DateLocal_24hr'  )           
        .filter(F.col('SampleDuration') == "24 HOUR")
        .groupBy('aqs_site_id_24hr','DateLocal_24hr')
        .agg(F.avg('ArithmeticMean').alias('measurement_dur24'))
    )
    
    # All "24 HR Block AVG" monitor measurement per day. 
    # Multiple daily measurements are averaged.       
    monitor_24blk_df = (
        pm25_nullify_zeros_df
        .select('aqs_site_id', 'DateLocal', 'ArithmeticMean')
        .withColumnRenamed('aqs_site_id', 'aqs_site_id_24hrblk')
        .withColumnRenamed('DateLocal'  , 'DateLocal_24hrblk'  )                
        .filter(F.col('SampleDuration') == "24-HR BLK AVG")
        .groupBy('aqs_site_id_24hrblk','DateLocal_24hrblk')
        .agg(F.avg('ArithmeticMean').alias('measurement_dur24blk'))
    )

    # All "1 HOUR" monitor measurement per day. 
    # Multiple daily measurements are averaged.
    # Only uses measurements with 24 observations counts.
    monitor__1hr_df = (
        pm25_nullify_zeros_df
        .select('aqs_site_id','DateLocal','ArithmeticMean')           
        .withColumnRenamed('aqs_site_id', 'aqs_site_id_1hr')
        .withColumnRenamed('DateLocal'  , 'DateLocal_1hr'  )
        .filter(F.col('SampleDuration') == "1 HOUR")
        .filter(F.col('ObservationCount') == 24)
        .groupBy('aqs_site_id_1hr','DateLocal_1hr')
        .agg(F.avg('ArithmeticMean').alias('measurement_dur1'))
    )

    # Merges all measurements to one row per measurement day.
    pm25_merge_df = (
        monitor_24hr_df    
        .join(  monitor_24blk_df, 
                ( F.col('aqs_site_id_24hr') == F.col('aqs_site_id_24hrblk') )   &
                ( F.col('DateLocal_24hr'  ) == F.col('DateLocal_24hrblk'  ) )   ,
                'fullouter'                                                                                 )
        .withColumn('aqs_site_id_a',  F.coalesce( F.col('aqs_site_id_24hr'), F.col('aqs_site_id_24hrblk') ) )
        .withColumn('date_a',         F.coalesce( F.col('DateLocal_24hr'  ), F.col('DateLocal_24hrblk'  ) ) )
        .join(  monitor__1hr_df, 
                ( F.col('aqs_site_id_a') == F.col('aqs_site_id_1hr') )   &
                ( F.col('date_a'       ) == F.col('DateLocal_1hr'  ) )   ,
                'fullouter'                                                                        )
        .withColumn('aqs_site_id',  F.coalesce( F.col('aqs_site_id_a'), F.col('aqs_site_id_1hr') ) )
        .withColumn('date',         F.coalesce( F.col('date_a'       ), F.col('DateLocal_1hr'  ) ) )                   
    ).select('aqs_site_id', 'date', 'measurement_dur24', 'measurement_dur24blk', 'measurement_dur1')  

    # Generate dates for every day for each monitor's first day 
    # of measurement through its last day of measurement.
    all_days_df = (
        pm25_merge_df
        .groupBy("aqs_site_id")
        .agg(F.min("date").alias("min_date"),
             F.max("date").alias("max_date"))
        .select("aqs_site_id", F.expr("sequence(min_date, max_date)").alias("date"))
        .withColumn("date", F.explode("date"))
        .withColumn("date", F.date_format("date", "yyyy-MM-dd"))        
    )

    # Join in rows for missing dates
    pm25_all_days_df = all_days_df.join(pm25_merge_df, ["aqs_site_id", "date"], "left")

    '''
    The final daily measurement for a monitor is chosen from the first, non-null
    value from the sample durations in the following order:
    1. 24 HOUR
    2. 24-HR BLK AVG
    3. 1 HOUR
    '''
    pm25_meas_df = (
        pm25_all_days_df
        .withColumn('measurement_avg', 
                    F.when(F.col('measurement_dur24').isNotNull(),      F.col('measurement_dur24'   ))
                     .when(F.col('measurement_dur24blk').isNotNull(),   F.col('measurement_dur24blk'))
                     .when(F.col('measurement_dur1').isNotNull(),       F.col('measurement_dur1'    ))
                     .otherwise(None)
        )
        .withColumn('measurement_avg', 
                    F.when(F.col('measurement_dur24').isNotNull(),      F.col('measurement_dur24'   ))
                     .when(F.col('measurement_dur24blk').isNotNull(),   F.col('measurement_dur24blk'))
                     .when(F.col('measurement_dur1').isNotNull(),       F.col('measurement_dur1'    ))
                     .otherwise(None)
        )        
    )

    # Create flags to indicate which sample duration measurement was used
    pm25_measurement_flags_df = (
        pm25_meas_df
        .withColumn('measurement_24hr',          
                    F.when(F.col('measurement_dur24').isNotNull()       ,   F.lit(1)).otherwise(F.lit(0)))
        .withColumn('measurement_24hr_blk_avg',
                    F.when(F.col('measurement_dur24blk').isNotNull()    &
                           F.col('measurement_dur24').isNull()          ,   F.lit(1)).otherwise(F.lit(0)))
        .withColumn('measurement_01hr',          
                    F.when(F.col('measurement_dur1').isNotNull()        &
                           F.col('measurement_dur24blk').isNull()       &
                           F.col('measurement_dur24').isNull()          ,   F.lit(1)).otherwise(F.lit(0)))          
    )
    
    df = pm25_measurement_flags_df

    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2c30fb2a-8470-42d4-a1e2-c3e6646b57d0"),
    monitors_nearby=Input(rid="ri.foundry.main.dataset.d7b6ccfc-c58a-4da7-8df6-61f554220cfc"),
    pm25_only=Input(rid="ri.foundry.main.dataset.f8ff70bf-e5e4-4113-8512-37b8b1e97738")
)
"""
All nearby PM 2.5 monitors
"""
def pm25_nearby_monitors(pm25_only, monitors_nearby):

    pm25_monitors_df    = pm25_only.select('aqs_site_id').dropDuplicates()
    monitors_nearby_df  = monitors_nearby.select('aqs_site_id').dropDuplicates()

    print(pm25_monitors_df.count())
    print(monitors_nearby_df.count())

    return monitors_nearby_df.join(pm25_monitors_df,'aqs_site_id','inner')
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f8ff70bf-e5e4-4113-8512-37b8b1e97738"),
    Daily_AirPollution_for_N3C=Input(rid="ri.foundry.main.dataset.2acf9cc2-855f-41fc-83e5-679382849ca9")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-05

Description:
Subsets air pollution data to PM 2.5 measurements      

Input:
1.  Daily_AirPollution_for_N3C 
    Daily air pollution measureemnts for many pollutants collected by EPA 
    monitors. Monitor locations are defined by Latitude and Longitude and
    ZCTAs. Patient ZIP Codes will be mapped to the monitor ZCTAs. 
================================================================================
"""

def pm25_only( Daily_AirPollution_for_N3C):
    EXTDATASET_104 = Daily_AirPollution_for_N3C

    # parameter code for PM 2.5
    parameter_code = 88101
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
        .filter(F.col('ParameterCode') == parameter_code)
    ) 
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.efe34955-db34-4705-b17c-e4410ff7aa1a"),
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
        # filters to ZCTA in East Harlem which is near 20 monitors
        df = zcta_monitor_df.filter(F.col('zcta') == '10035')
    else:
        df = zcta_monitor_df

    return df    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8abd6881-6e96-4d78-b677-b669da0a0c88"),
    monitors_nearby=Input(rid="ri.foundry.main.dataset.d7b6ccfc-c58a-4da7-8df6-61f554220cfc"),
    pm25_aggregations=Input(rid="ri.foundry.main.dataset.6504028c-c02d-4696-8d6f-0c9104f4798f")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-07-07

Description:
Joins zip_code/zcta with all daily air pollution observations

Input:
1.  pm25_aggregations
    All daily measurements per monitor

2.  monitors_nearby    
    nearby monitors to patient-based Zip Code / ZCTA
================================================================================
"""
def zip_all_monitor_obs(monitors_nearby, pm25_aggregations):

    zip_measurement_df = (
            pm25_aggregations
            .join(monitors_nearby, 'aqs_site_id', 'left')
            .filter(F.col('zip_code').isNotNull())
    )

    return zip_measurement_df
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a3cef816-9f77-45ff-b10d-932c73bdc497"),
    pm25_only=Input(rid="ri.foundry.main.dataset.f8ff70bf-e5e4-4113-8512-37b8b1e97738"),
    zip_all_monitor_obs=Input(rid="ri.foundry.main.dataset.8abd6881-6e96-4d78-b677-b669da0a0c88")
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
        - 24 HOUR
        - 24 HR BLK AVG
        - 1 HR

2. Creates a count of non-null measurements and measurement averages for:
   - the  30 calendar days prior to current date 
   - the 365 calendar days prior to current date

Input:
1.  zip_all_monitor_obs
    zip_code/zcta with all daily air pollution observations
2.  pm25_only
    PM 2.5 pollutant data. Used to add ParameterName and UnitsofMeasure columns.
================================================================================
"""
def zip_daily_obs(zip_all_monitor_obs, pm25_only):

    # Aggregate values by [zip_code, date]
    zamo_agg_df = (
        zip_all_monitor_obs
        .groupBy('zip_code', 'date')
        .agg(F.avg('measurement_avg'            ).alias('measurement_avg')  ,
             F.sum('measurement_24hr'           ).alias('num_24hr_obs'   )  ,   
             F.sum('measurement_24hr_blk_avg'   ).alias('num_24hrblk_obs')  ,
             F.sum('measurement_01hr'           ).alias('num_01hr_obs'   )  ,
             F.count('measurement_avg'          ).alias('num_monitors'   )  )
        .select('zip_code'          , 
                'date'              ,                
                'measurement_avg'   , 
                'num_monitors'      , 
                'num_24hr_obs'      , 
                'num_24hrblk_obs'   , 
                'num_01hr_obs'      )  
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

    # Get PM 2.5 ParameterName and UnitsofMeasure from first rows
    # of PM 2.5 pollution data and join to result set  
    pm25_name_units_df = pm25_only.select('ParameterName', 'ParameterCode', 'UnitsofMeasure').limit(1)
    pm25_df = zamo_prev_counts_df.join(pm25_name_units_df)

    df = (
        pm25_df
        .select('zip_code'                  , 
                'date'                      , 
                'ParameterName'             , 
                'ParameterCode'             ,
                'UnitsofMeasure'            ,
                'measurement_avg'           , 
                "num_monitors"              ,
                "num_24hr_obs"              ,
                "num_24hrblk_obs"           ,
                "num_01hr_obs"              ,
                "prev_30_day_avg"           ,
                "prev_30_days_w_obs"        ,
                "prev_365_day_avg"          ,
                "prev_365_days_w_obs"       )
        .withColumnRenamed('date', 'meas_date')
    )  

    return df

    

