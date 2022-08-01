from pyspark.sql import functions as F
from pyspark.sql.window import Window

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0937fbc2-c1f8-425d-98aa-884aa47f6492"),
    zip_daily_obs=Input(rid="ri.foundry.main.dataset.a3cef816-9f77-45ff-b10d-932c73bdc497")
)
def PM25_DAILY_OBS(zip_daily_obs):
    
    df = zip_daily_obs

    return zip_daily_obs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.d7b6ccfc-c58a-4da7-8df6-61f554220cfc"),
    pf_zip_zcta=Input(rid="ri.foundry.main.dataset.a097770a-3f86-4a5e-9171-aa185b0ad50a"),
    zcta_monitors_filter=Input(rid="ri.foundry.main.dataset.efe34955-db34-4705-b17c-e4410ff7aa1a")
)
"""
Joins nearby monitors to patient-based Zip Code / ZCTA
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
def pf_zip_code(COVID_POS_PERSON_FACT):

    """
    TO DO: 
    Get distinct zip_code values from COVID_POS_PERSON_FACT. 
    The zip_code values will be associated with ZCTAs in the 
    next step. That's all!
    """

    # returns one person from zip_code = 10035, born in 1935, has covid-associated hospitalization, etc., 
    return COVID_POS_PERSON_FACT.filter(F.col('person_id') == '2995272647743632189').select('zip_code')
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a097770a-3f86-4a5e-9171-aa185b0ad50a"),
    ZiptoZcta_Crosswalk_2021=Input(rid="ri.foundry.main.dataset.e0b7c411-d034-4828-a522-e9709f3e3746"),
    pf_zip_code=Input(rid="ri.foundry.main.dataset.a66c9f77-bb1f-4943-b0b6-338797130827")
)
"""
Adds associated ZCTAs for cohort based on their Zip Codes
"""
def pf_zip_zcta(ZiptoZcta_Crosswalk_2021, pf_zip_code):
    
    # lower case column names
    zip_to_zcta_df = ZiptoZcta_Crosswalk_2021.select(F.col('ZIP_CODE').alias('zip_code'), F.col('ZCTA').alias('zcta'))

    # join zcta to zip_code
    df = pf_zip_code.join(zip_to_zcta_df, 'zip_code', 'left').select('zip_code', 'zcta')

    return df 
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6504028c-c02d-4696-8d6f-0c9104f4798f"),
    pm25_only=Input(rid="ri.foundry.main.dataset.f8ff70bf-e5e4-4113-8512-37b8b1e97738")
)
def pm25_aggregations(pm25_only):

    # There are negative and zero ArithmeticMean values. 
    # All such values ( < 0.00001) are set to null to 
    # insure they are not used in calculating averages. 
    pm25_nullify_zeros_df =(
        pm25_only
        .withColumn('ArithmeticMean', F.when(F.col('ArithmeticMean') < 0.00001, None).otherwise(F.col('ArithmeticMean')))
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
        .select('aqs_site_id', 'date', 'measurement_dur24', 'measurement_dur24blk', 'measurement_dur1')                      
    )

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
    Gets average of three measurement column:
    denominator    : gets a count of the non-null measurement columns
    measurement_avg: adds non-null measurements (converts null to zero)
                     and divides by denominator
    '''
    pm25_averages_df = (
        pm25_all_days_df
        .withColumn('denominator',  
                    sum(F.col(i).isNotNull().cast('int') 
                        for i in pm25_all_days_df.columns
                           if i in('measurement_dur24', 'measurement_dur24blk', 'measurement_dur1')))
        .withColumn('measurement_avg', 
                    F.when(F.col('denominator') > 0, 
                        (F.coalesce(F.col('measurement_dur24'),     F.lit(0)) +   
                         F.coalesce(F.col('measurement_dur24blk'),  F.lit(0)) +   
                         F.coalesce(F.col('measurement_dur1'),      F.lit(0)) )/F.col('denominator'))
                    .otherwise(None))
        .drop('denominator')            
    )

    # Create flags for presence of each type of measurement
    pm25_measurement_flags_df = (
        pm25_averages_df
        .withColumn('measurement_24hr',          
                    F.when(F.col('measurement_dur24').isNotNull(),      F.lit(1)).otherwise(F.lit(0)))
        .withColumn('measurement_24hr_blk_avg',
                    F.when(F.col('measurement_dur24blk').isNotNull(),   F.lit(1)).otherwise(F.lit(0)))
        .withColumn('measurement_01hr',          
                    F.when(F.col('measurement_dur1').isNotNull(),       F.lit(1)).otherwise(F.lit(0)))        
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
    Daily_AirPollution_for_N3C=Input(rid="ri.foundry.main.dataset.5cbfc3ea-297e-49e6-9454-df070e93c75b")
)
"""
Subsets air pollution data to PM 2.5 measurements
PM 2.5 ParameterCode = 88101  
Ozone  ParameterCode = 44201  

"""
def pm25_only(Daily_AirPollution_for_N3C):

    parameter_code = 88101

    df = (
        Daily_AirPollution_for_N3C
        .select('aqs_site_id','DateLocal','ArithmeticMean','UnitsofMeasure','SampleDuration','ObservationCount','ParameterCode','ParameterName','CityName','StateName', 'Latitude', 'Longitude')
        .filter(F.col('ParameterCode') == parameter_code)
    ) 
    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.efe34955-db34-4705-b17c-e4410ff7aa1a"),
    ZCTA_MONITOR=Input(rid="ri.foundry.main.dataset.3fc0ff9c-1f83-47de-b46a-110cc8fb9ed7")
)
def zcta_monitors_filter(ZCTA_MONITOR):
    
    
    """
    TO DO: 
    Set to return all 
    """

    # Currently filters to ZCTA in East Harlem which is near 20 monitors
    return ZCTA_MONITOR.filter(F.col('zcta') == '10035').withColumnRenamed('monitor', 'aqs_site_id')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8abd6881-6e96-4d78-b677-b669da0a0c88"),
    monitors_nearby=Input(rid="ri.foundry.main.dataset.d7b6ccfc-c58a-4da7-8df6-61f554220cfc"),
    pm25_aggregations=Input(rid="ri.foundry.main.dataset.6504028c-c02d-4696-8d6f-0c9104f4798f")
)
'''
Joins zip_code/zcta with all daily air pollution observations  
'''
def zip_all_monitor_obs(monitors_nearby, pm25_aggregations):

    zip_measurement_df = (
            pm25_aggregations
            .join(monitors_nearby, 'aqs_site_id', 'left')
            .filter(F.col('zip_code').isNotNull())
            #.filter(F.col('aqs_site_id') == '34-003-0003')
    )

    return zip_measurement_df
    
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a3cef816-9f77-45ff-b10d-932c73bdc497"),
    zip_all_monitor_obs=Input(rid="ri.foundry.main.dataset.8abd6881-6e96-4d78-b677-b669da0a0c88")
)
"""
1. Rolls up observation by [zip_code, date] and produces:
   - the count of monitors with observations per day
   - the average of all monitors per day
   - the number of 24 HOUR durations observations per day
   - the number of 24 HOUR BLK AVG durations observations per day
   - the number of  1 HOUR durations observations per day

2. Creates a count of non-null measurements and measurement averages for:
   - the  30 calendar days prior to current date 
   - the 365 calendar days prior to current date
"""

def zip_daily_obs(zip_all_monitor_obs):

    # Aggregate values by [zip_code, date]
    zamo_agg_df = (
        zip_all_monitor_obs
        .groupBy('zip_code', 'date')
        .agg(F.avg('measurement_avg'            ).alias('pm25_meas_avg'  )  ,
             F.count('date'                     ).alias('num_monitors'   )  ,
             F.sum('measurement_24hr'           ).alias('num_24hr_obs'   )  ,   
             F.sum('measurement_24hr_blk_avg'   ).alias('num_24hrblk_obs')  ,
             F.sum('measurement_01hr'           ).alias('num_01hr_obs'   )  )
        .select('zip_code', 
                'date', 
                'pm25_meas_avg', 
                'num_monitors', 
                'num_24hr_obs', 
                'num_24hrblk_obs', 
                'num_01hr_obs')  
    )

    # function used in following Window funtions to count seconds in a day
    days = lambda i: i * 86400 

    # Creates window by casting timestamp to long (number of seconds) for previous 30 days
    w = (Window.orderBy(F.col("date").cast("timestamp").cast('long')).rangeBetween(-days(30), -1))

    # Rolling averages and counts of measurements for previous 30 days
    zamo_prev_avg_df = (
        zamo_agg_df
        .withColumn('prev_30_day_avg',      F.avg("pm25_meas_avg"  ).over(w) )    
        .withColumn('prev_30_days_w_obs',   F.count("pm25_meas_avg").over(w) )    
    )

    # Creates window by casting timestamp to long (number of seconds) for previous 365 days
    w = (Window.orderBy(F.col("date").cast("timestamp").cast('long')).rangeBetween(-days(365), -1))

    # Rolling averages and counts of measurements for previous 365 days
    zamo_prev_counts_df = (
        zamo_prev_avg_df
        .withColumn('prev_365_day_avg',      F.avg("pm25_meas_avg"  ).over(w) )    
        .withColumn('prev_365_days_w_obs',   F.count("pm25_meas_avg").over(w) )    
    )

    return zamo_prev_counts_df.withColumnRenamed('date', 'meas_date')

    

