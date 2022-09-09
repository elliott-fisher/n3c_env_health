from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.203392f0-b875-453c-88c5-77ca5223739e"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86"),
    our_concept_sets=Input(rid="ri.foundry.main.dataset.f80a92e0-cdc4-48d9-b4b7-42e60d42d9e0"),
    pf_clean=Input(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-30

Description:
Result table contains one row per patient with recorded comorbidities with the 
following two sets of comorbidity columns:
    - columns that contain the first recorded date of the comorbidity condition
    - columns that contain the condition_occurrence_id of the record in the 
      condition_occurrence table for the first recorded date 

The comorbidity concept names are retrieved from the our_concept_sets table 
and added as columns to the result table. If a patient has a comorbidity, the
earliest comorbidity start date is recorded in the comorbidity column.

Notes:
- Comorbidities (from condition_occurrence) with null condition_start_date 
  values are dropped

Input:

pf_clean:
The preceding patient table in the code workbook.

our_concept_sets:
This table is built from a manually built spreadsheet (a Palantir Foundrey 
"Fusion Table"), and contains concept_set_members for the comorbidities of 
interest. 

concept_set_members:
Used to find most recent concept_id values for the comorbidity concept_set_names
in our_concept_sets

condition_occurrence:
Details of all patients conditions.
================================================================================
"""
def COVID_PATIENT_COMORBIDITIES(pf_clean, our_concept_sets, condition_occurrence, concept_set_members):
    
    pf_covid_date_df = pf_clean.select('person_id', 'first_pos_pcr_antigen_date')
    

    # Get comorbidity concept_set_name values from our list 
    comorbidity_concept_names_df = (
        our_concept_sets
            .filter(our_concept_sets.domain.contains('condition_occurrence'))
            .filter(our_concept_sets.comorbidity == 1)
            .select('concept_set_name','column_name')
    )

    # Get most recent version of comorbidity concept_id values from concept_set_members 
    comorbidity_concept_set_members_df = (
        concept_set_members
            .select('concept_id','is_most_recent_version','concept_set_name')
            .where(F.col('is_most_recent_version') == 'true')
            .join(comorbidity_concept_names_df, 'concept_set_name', 'inner')
            .select('concept_id','column_name')
    )

    """ 
    Get all conditions for current set of Covid+ patients    
    where the condition_start_date is not null
    """
    person_conditions_df = (
        condition_occurrence 
            .select('person_id'                 , 
                    'condition_start_date'      , 
                    'condition_concept_id'      , 
                    'condition_occurrence_id'   , 
                    'condition_source_value'    )  
            .where(F.col('condition_start_date').isNotNull()) 
            .withColumnRenamed('condition_concept_id','concept_id') # renamed for next join
            .join(pf_covid_date_df,'person_id','inner')
    )

    # Subset person_conditions_df to records with comorbidities
    person_comorbidities_df = (
        person_conditions_df
            .join(comorbidity_concept_set_members_df, 'concept_id', 'inner')
            .withColumnRenamed('condition_start_date','comorbidity_start_date')
            .withColumnRenamed('column_name','comorbidity')
            .select('person_id'                     , 
                    'comorbidity'                   ,
                    'comorbidity_start_date'        , 
                    'first_pos_pcr_antigen_date'    , 
                    'condition_occurrence_id'       , 
                    'condition_source_value'        )
    ) 

    # Only keep the record with the earliest comorbidity_start_date
    w = Window.partitionBy('person_id', 'comorbidity').orderBy('comorbidity_start_date')
    earliest_comorbidity_df = (
        person_comorbidities_df
        .withColumn('comorbidity_start_date'    , F.first('comorbidity_start_date').over(w)     )
        .withColumn('first_pos_pcr_antigen_date', F.first('first_pos_pcr_antigen_date').over(w) )  
        .withColumn('condition_occurrence_id'   , F.first('condition_occurrence_id').over(w)    )  
        .withColumn('condition_source_value'    , F.first('condition_source_value').over(w)     ) 
    ).dropDuplicates()

    # Transpose the comorbidity value to the following two columns:
    # - a column and with comorbidity_start_date values
    # - a column with corresponding condition_occurrence_id values 
    # Note: F.first() works because there is only one date per person per comorbidity 
    person_comorbidity_start_df = (
        earliest_comorbidity_df
            .groupby('person_id')
            .pivot('comorbidity')
            .agg(F.first('comorbidity_start_date').alias('start')       ,
                 F.first('condition_occurrence_id').alias('c_occ_id')   )
    )

    return person_comorbidity_start_df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ece71c14-f69d-4d62-b445-820677315469"),
    pf_mds=Input(rid="ri.foundry.main.dataset.ef1715b2-121f-4129-86c2-e8cc3759e432")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-31

Description:
This should always precede the COVID_POS_PERSON_FACT node. 

Input:
The preceding patient table in the code workbook.
================================================================================
"""
def COVID_PATIENT_SDOH(pf_mds):

    df = (
        pf_mds
        .withColumn("African_American",F.col('African_American').cast('double'))       
        .withColumn('AIAN',F.col('AIAN').cast('double'))
        .withColumn('Asian',F.col('Asian').cast('double'))
        .withColumn('NHPI',F.col('NHPI').cast('double'))
        .withColumn('White',F.col('White').cast('double'))
        .withColumn('other_race',F.col('other_race').cast('double'))
        .withColumn('two_or_more_races',F.col('two_or_more_races').cast('double'))
        .withColumn('Hispanic_any_race',F.col('Hispanic_any_race').cast('double'))
        .withColumn('male',F.col('male').cast('double'))
        .withColumn('female',F.col('female').cast('double'))
        .withColumn('age_18_and_under',F.col('age_18_and_under').cast('double'))
        .withColumn('age_18_and_over',F.col('age_18_and_over').cast('double'))
        .withColumn('age_65plus',F.col('age_65plus').cast('double'))
    ) 
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810"),
    pf_death=Input(rid="ri.foundry.main.dataset.b0eef012-2041-4a78-9289-610f2661f10a")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-30

Description:
This produces a copy of the preceding node's dataset. It is used for naming
purposes.

Input:
pf_mds:
The preceding patient table in the code workbook.
================================================================================
"""
def COVID_POS_PERSON_FACT(pf_death):

    # Convert String values to numbers
    df = pf_death

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ce1538cf-712c-492a-8f8c-adb5afa71b08"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def explore_m_to_m(microvisit_to_macrovisit_lds):

    # Counts of distinct dates within macrovisits --> looks like there are no variations
    """
    df = (
        microvisit_to_macrovisit_lds
        .select('person_id', 'visit_concept_id', 'visit_concept_name', 'macrovisit_id',  'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
        .groupby('macrovisit_id')
        .agg(F.countDistinct(F.col('macrovisit_start_date')).alias('distinct_start_dates'))        
    )
    """

    df = (
        microvisit_to_macrovisit_lds
        .select('person_id', 'visit_concept_id', 'visit_concept_name', 'macrovisit_id',  'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
        .where(F.col('visit_concept_id') == 9201)        
        .groupby('macrovisit_id')
        .agg(F.count(F.col('visit_concept_id')).alias('multi_inpatient_codes'))        
    ) 
       
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.20f4ec4e-7cf2-4b4f-ba5f-7a28059f2105"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def macrovisit_multi_ip(microvisit_to_macrovisit_lds):

    df = (
        microvisit_to_macrovisit_lds
        .select(
            'person_id',
            'macrovisit_id', 
            'visit_occurrence_id', 
            'visit_concept_id',
            'visit_concept_name', 
            'visit_start_date', 
            'visit_end_date',
            'macrovisit_start_date',
            'macrovisit_end_date'
        )
        .filter(F.col('macrovisit_id') == "4659206354756305685_1_969748783")
        .sort('visit_concept_id')
    )

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.503efade-9d86-453b-9071-804172e87222")
)
"""
================================================================================
Description:
Adds hospitalization start and end dates for all hospitalization after the 
the COVID-associated hospitalization  

Index Date is defined by first_poslab_or_diagnosis_date
*** DICSUSS or ADD FLEXIBILITY ----> Global Variables??
================================================================================ 
"""
def pf_after_covid_visits():

    macrovisits_df  = (
        microvisit_to_macrovisit_lds
        .select('person_id', 'macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
    )    

    pf_has_covid_hosp_df = (
        pf_covid_visits
        .select('person_id', 'first_poslab_or_diagnosis_date', 'first_COVID_hospitalization_start_date','first_COVID_hospitalization_end_date')
        .filter(F.col('first_COVID_hospitalization_start_date').isNotNull())
    )

    pf_no_covid_hosp_df = (
        pf_covid_visits
        .select('person_id', 'first_poslab_or_diagnosis_date', 'first_COVID_hospitalization_start_date','first_COVID_hospitalization_end_date')
        .filter(F.col('first_COVID_hospitalization_start_date').isNull())
    )

    w = Window.partitionBy('person_id', 'macrovisit_id').orderBy('macrovisit_start_date')

    has_df = (
        pf_has_covid_hosp_df
        .join(macrovisits_df, 'person_id', 'inner')
        .where(F.col('first_COVID_hospitalization_start_date') < F.col('macrovisit_start_date'))
        .withColumn('macrovisit_id',            F.first('macrovisit_id').over(w))
        .withColumn('macrovisit_start_date',    F.first('macrovisit_start_date').over(w))
        .withColumn('macrovisit_end_date',      F.first('macrovisit_end_date').over(w))
        .dropDuplicates()        
    )
    print(has_df.count())

    no_df = (
        pf_no_covid_hosp_df
        .join(macrovisits_df, 'person_id', 'inner')
        .where(F.col('first_poslab_or_diagnosis_date') < F.col('macrovisit_start_date'))
        .withColumn('macrovisit_id',            F.first('macrovisit_id').over(w))
        .withColumn('macrovisit_start_date',    F.first('macrovisit_start_date').over(w))
        .withColumn('macrovisit_end_date',      F.first('macrovisit_end_date').over(w))
        .dropDuplicates()        
    )
    print(no_df.count())

    df = has_df.union(no_df)
    print(df.count())

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e"),
    pf_locations=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-03

Description:
Cleans and transforms data in the following ways:
- Changes null values for month day year of birthdate to 1 
  for calculating new column date_of_birth
- Creates age_at_first_covid_diag column 
- Creates gender column and uppercases all mixed/lowercase
  versions of "male" and "female" to MALE and FEMALE; sets
  all other to "UNKNOWN"
- Cleans zip_code values
- Creates race_ethnicity column

Input:

Output:
================================================================================
"""
def pf_clean(pf_locations):

    pf_df = pf_locations

    """
    Creates columns: date_of_birth, age_at_first_covid_diag  
    Note: Sets null values of the following to 1:
        - year_of_birth
        - month_of_birth
        - day_of_birth
    """
    with_dob_age_df = (
        pf_df
            .withColumn("new_year_of_birth",  
                        F.when(pf_df.year_of_birth.isNull(),1)
                        .otherwise(pf_df.year_of_birth))
            .withColumn("new_month_of_birth", 
                        F.when(pf_df.month_of_birth.isNull(),1)
                        .otherwise(pf_df.month_of_birth))
            .withColumn("new_day_of_birth", 
                        F.when(pf_df.day_of_birth.isNull(),1)
                        .otherwise(pf_df.day_of_birth))
            .withColumn("date_of_birth", 
                        F.concat_ws("-", F.col("new_year_of_birth"), F.col("new_month_of_birth"), F.col("new_day_of_birth")))
            .withColumn("date_of_birth", 
                        F.to_date("date_of_birth", format=None))
            .withColumn("age_at_first_covid_diag", 
                        F.floor(F.months_between("first_pos_pcr_antigen_date", "date_of_birth", roundOff=False)/12))
            .withColumn("age_at_first_covid_diag", 
                        F.when(F.col("age_at_first_covid_diag") >=0, F.col("age_at_first_covid_diag")).otherwise(F.lit(None)))  # insert null if age is negative
    ).drop('new_year_of_birth','new_month_of_birth','new_day_of_birth')
    

    """
    Creates column: gender
    Contains standardized values from gender_concept_name so that:
    - Uppercase all versions of "male" and "female" strings
    - Replace non-MALE and FEMALE values with UNKNOWN 
    """
    cpp_gender_df = (
        with_dob_age_df
            .withColumn("gender",  
                F.when(F.upper(with_dob_age_df.gender_concept_name) == "MALE", "MALE")
                .when(F.upper(with_dob_age_df.gender_concept_name) == "FEMALE", "FEMALE")
                .otherwise("UNKNOWN")
            )
    )

    """
    Creates column: race_ethnicity
    Contains standardized values from ethnicity_concept_name and race_concept_name

    In data, but currentally set to UNKNOWN
    Barbadian
    Dominica Islander
    Trinidadian
    West Indian
    Jamaican
    African
    Madagascar
    Maldivian
    """
    cpp_race_df = ( 
        cpp_gender_df
            .withColumn("race_ethnicity", 
                F.when(F.col("ethnicity_concept_name") == 'Hispanic or Latino',     "Hispanic or Latino Any Race")
                .when(F.col("race_concept_name").contains('Hispanic'),              "Hispanic or Latino Any Race")
                .when(F.col("race_concept_name").contains('Black'),                 "Black or African American Non-Hispanic")
                .when(F.col("race_concept_name") == ('African American'),           "Black or African American Non-Hispanic")                
                .when(F.col("race_concept_name").contains('White'),                 "White Non-Hispanic")
                .when(F.col("race_concept_name") == "Asian or Pacific Islander",    "Unknown") 
                .when(F.col("race_concept_name").contains("Asian Indian"),          "Asian Indian")
                .when(F.col("race_concept_name").contains('Asian'),                 "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Filipino'),              "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Chinese'),               "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Korean'),                "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Vietnamese'),            "Asian Non-Hispanic")
                .when(F.col("race_concept_name").contains('Japanese'),              "Asian Non-Hispanic")                  
                .when(F.col("race_concept_name").contains('Bangladeshi'),           "Asian Non-Hispanic") 
                .when(F.col("race_concept_name").contains('Pakistani'),             "Asian Non-Hispanic")   
                .when(F.col("race_concept_name").contains('Nepalese'),              "Asian Non-Hispanic")    
                .when(F.col("race_concept_name").contains('Laotian'),               "Asian Non-Hispanic")     
                .when(F.col("race_concept_name").contains('Taiwanese'),             "Asian Non-Hispanic")                                        
                .when(F.col("race_concept_name").contains('Thai'),                  "Asian Non-Hispanic")        
                .when(F.col("race_concept_name").contains('Sri Lankan'),            "Asian Non-Hispanic")      
                .when(F.col("race_concept_name").contains('Burmese'),               "Asian Non-Hispanic")      
                .when(F.col("race_concept_name").contains('Okinawan'),              "Asian Non-Hispanic")                                                            
                .when(F.col("race_concept_name").contains('Cambodian'),             "Asian Non-Hispanic")   
                .when(F.col("race_concept_name").contains('Bhutanese'),             "Asian Non-Hispanic")   
                .when(F.col("race_concept_name").contains('Singaporean'),           "Asian Non-Hispanic") 
                .when(F.col("race_concept_name").contains('Hmong'),                 "Asian Non-Hispanic")       
                .when(F.col("race_concept_name").contains('Malaysian'),             "Asian Non-Hispanic")    
                .when(F.col("race_concept_name").contains('Indonesian'),            "Asian Non-Hispanic")                 
                .when(F.col("race_concept_name").contains('Pacific'),               "Native Hawaiian or Other Pacific Islander Non-Hispanic")
                .when(F.col("race_concept_name").contains('Polynesian'),            "Native Hawaiian or Other Pacific Islander Non-Hispanic")        
                .when(F.col("race_concept_name").contains('Native Hawaiian'),       "Native Hawaiian or Other Pacific Islander Non-Hispanic")  
                .when(F.col("race_concept_name").contains('Micronesian'),           "Native Hawaiian or Other Pacific Islander Non-Hispanic")     
                .when(F.col("race_concept_name").contains('Melanesian'),            "Native Hawaiian or Other Pacific Islander Non-Hispanic")       
                # .when(F.col("race_concept_name").contains('Other'),                 "UNKNOWN")    
                # .when(F.col("race_concept_name").contains('Multiple'),              "UNKNOWN")  
                # .when(F.col("race_concept_name").contains('More'),                  "UNKNOWN")      
                .otherwise("UNKNOWN")
            )
    )

    """
    Creates column: zip_code
    Standardizes the values in zip:
    1. removes leading and training blanks
    2. truncates to first five characters
    3. only keeps values with 5 digit characters 
    """
    cpp_zip_df = ( 
        cpp_race_df
            .withColumn("zip_code", F.trim(cpp_race_df.zip))
            .withColumn("zip_code", F.when(F.length(F.col('zip_code')) >=  5, F.col('zip_code').substr(1,5)))
            .withColumn("zip_code", F.when(F.col('zip_code').rlike("[0-9]{5}"), F.col('zip_code')))
    )

    return cpp_zip_df

#################################################
## Global imports and functions included below ##
#################################################

from pyspark.sql.window import Window
from pyspark.sql import functions as F

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f59a2725-002c-4fb3-b069-1f3927620b40")
)
"""
================================================================================
Description:
Adds hospitalization start and end dates and optionally Emergency Room visit 
dates. (To get both sets of dates, set get_er_and_hosp_visits == True)  
================================================================================ 
"""
def pf_covid_visits_dep():

    """
    ================================================================================
    Potential Parameters 
    --------------------
    get_er_and_hosp_visits (boolean)
    False - gets only hospitalizations
    True  - gets hospitalizations and emergency room visits 

    requires_lab_and_diagnosis (boolean)
    True  - first poslab date AND diag date are used to associate with visits
    False - first poslab date OR diag date are used to associate with visits

    num_days_before / num_days_after (int)
    Proximity in days between index date(s) and visit date
    *** NEEDS DISCUSSION ***  
    ================================================================================ 
    """
    get_er_and_hosp_visits      = True    
    requires_lab_and_diagnosis  = False
    num_days_before             = 1
    num_days_after              = 16

    pf_df = pf_comorbidities

    # Reduce patient columns and create column with the number of 
    # of days between the poslab and diagnosis dates
    pf1_df = (
        pf_df
            .select('person_id', 'first_pos_pcr_antigen_date', 'first_pos_diagnosis_date', 'first_poslab_or_diagnosis_date')
            .withColumn('poslab_minus_diag_date', F.datediff('first_pos_pcr_antigen_date', 'first_pos_diagnosis_date'))
    )

    # Reduce microvisit_to_macrovisit_lds columns and joined to contain patients
    pf_visits_df = (
        microvisit_to_macrovisit_lds
            .select('person_id','visit_start_date','visit_concept_id', 'macrovisit_id', 'macrovisit_start_date','macrovisit_end_date')
            .join(pf1_df,'person_id','inner')  
    )

    """
    ================================================================================
    Get list of Emergency Room Visit concept_set_name values from our spreadsheet 
    and use to create a list of associated concept_id values  
    ================================================================================
    """
    er_concept_names = list(
        our_concept_sets
            .filter(our_concept_sets.er_only_visit == 1)
            .select('concept_set_name').toPandas()['concept_set_name']
    )    
    er_concept_ids = (
        list(concept_set_members
                .where(( concept_set_members.concept_set_name.isin(er_concept_names)) & 
                       ( concept_set_members.is_most_recent_version == 'true'))
                .select('concept_id').toPandas()['concept_id']
        )
    )
    print(er_concept_ids)

    """
    ================================================================================ 
    Get Emergency Room visits (null macrovisit_start_date values and is in 
    er_concept_ids) and
    create the following columns: 
    poslab_minus_ER_date      - used for hospitalizations that require *both*
                                poslab and diagnosis
    first_index_minus_ER_date - used for hospitalizations that require *either*
                                poslab or diagnosis
    ================================================================================                                
    """
    er_df = (
        pf_visits_df
            .where(pf_visits_df.macrovisit_start_date.isNull() & (pf_visits_df.visit_concept_id.isin(er_concept_ids)))
            .withColumn('poslab_minus_ER_date', 
                F.datediff('first_pos_pcr_antigen_date',     'visit_start_date'))
            .withColumn("first_index_minus_ER_date", 
                F.datediff("first_poslab_or_diagnosis_date", "visit_start_date"))         
    )

    """
    ================================================================================

    GET ER VISITS    

    ================================================================================
    """
    if requires_lab_and_diagnosis == True:
        er_df = (
            er_df
                .withColumn("poslab_associated_ER", 
                             F.when(F.col('poslab_minus_ER_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .withColumn("poslab_and_diag_associated_ER", 
                             F.when((F.col('poslab_associated_ER') == 1) & 
                                    (F.col('poslab_minus_diag_date').between(-num_days_after, num_days_before)), 1).otherwise(0)
                )
                .where(F.col('poslab_and_diag_associated_ER') == 1)
                .withColumnRenamed('visit_start_date', 'covid_ER_only_start_date')
                .select('person_id', 'covid_ER_only_start_date')
                .dropDuplicates()
        )     
    else:
        er_df = (
            er_df
                .withColumn("poslab_or_diag_associated_ER", 
                             F.when(F.col('first_index_minus_ER_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .where(F.col('poslab_or_diag_associated_ER') == 1)
                .withColumnRenamed('visit_start_date', 'covid_ER_only_start_date')                             
                .select('person_id', 'covid_ER_only_start_date')
                .dropDuplicates()
        )        

    # get first er visit within range of Covid index date
    first_er_df = (
        er_df
        .groupBy('person_id')
        .agg(F.min('covid_ER_only_start_date').alias('first_covid_er_only_start_date'))    
    )

    """ 
    ================================================================================    
    Get Hospitalization visits (non-null macrovisit_start_date values) and
    create the following columns: 
    poslab_minus_hosp_date      - used for hospitalizations that require *both*
                                  poslab and diagnosis
    first_index_minus_hosp_date - used for hospitalizations that require *either*
                                  poslab or diagnosis
    ================================================================================                                  
    """
    all_hosp_df = (
        pf_visits_df
            .where(pf_visits_df.macrovisit_start_date.isNotNull())
            .withColumn("poslab_minus_hosp_date", 
                F.datediff("first_pos_pcr_antigen_date",     "macrovisit_start_date"))
            .withColumn("first_index_minus_hosp_date", 
                F.datediff("first_poslab_or_diagnosis_date", "macrovisit_start_date"))    
    )

    """
    ================================================================================
    To have a hospitalization associated with *both* Positive PCR/Antigen test  
    and Covid Diagnosis, the test and diagnosis date need to be close together
    and the test and hospitalization must be close together. 
    
    Specifically:
    1. The hosp date must be within [num_days_before, num_days_after] of the poslab date 
       AND
    2. The diag date must be within [num_days_before, num_days_after] of the poslab date

    Example:
    ==============================================================
    [num_days_before, num_days_after] = [1,16]
    poslab date     = June 10 [June 9, June 16]
    diag date       = June 12 
    hosp date       = June 22
    
    1. Hospitalization must occur between June 9 and June 26: true
    2. Diagnosis date  must occur between June 9 and June 26: true
    ==============================================================

    Otherwise, to get hospitalizations associated with *either* a positive 
    PCR/Antigen test *or* a positive Covid Diagnosis, the first index date
    (whichever comes first:  PCR/Antigen or Diagnosis date) and the 
    hospitalization date must be close together. 
    ================================================================================    
    """
    if requires_lab_and_diagnosis == True:
        hosp_df = (
            all_hosp_df
                .withColumn("poslab_associated_hosp", 
                             F.when(F.col('poslab_minus_hosp_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .withColumn("poslab_and_diag_associated_hosp", 
                             F.when( (F.col('poslab_associated_hosp') == 1) & 
                                     (F.col('poslab_minus_diag_date').between(-num_days_after, num_days_before)), 1).otherwise(0)
                )
                .where(F.col('poslab_and_diag_associated_hosp') == 1)
                .withColumnRenamed('macrovisit_start_date', 'covid_hospitalization_start_date')
                .withColumnRenamed('macrovisit_end_date',   'covid_hospitalization_end_date')
                .select('person_id', 'macrovisit_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date')
                .dropDuplicates()
        )     
    else:
        hosp_df = (
            all_hosp_df
                .withColumn("poslab_or_diag_associated_hosp", 
                             F.when(F.col('first_index_minus_hosp_date').between(-num_days_after, num_days_before), 1).otherwise(0)
                )
                .where(F.col('poslab_or_diag_associated_hosp') == 1)
                .withColumnRenamed('macrovisit_start_date', 'covid_hospitalization_start_date')
                .withColumnRenamed('macrovisit_end_date',   'covid_hospitalization_end_date')          
                .select('person_id', 'macrovisit_id', 'covid_hospitalization_start_date', 'covid_hospitalization_end_date')
                .dropDuplicates()
        )
    
    # get first hospitalization period within Covid index date range 
    w = Window.partitionBy('person_id').orderBy('covid_hospitalization_start_date')

    first_hosp_df = (
        hosp_df
        .withColumn('macrovisit_id', F.first('macrovisit_id').over(w))
        .withColumn('first_COVID_hospitalization_start_date', F.first('covid_hospitalization_start_date').over(w))
        .withColumn('first_COVID_hospitalization_end_date',   F.first('covid_hospitalization_end_date').over(w))
        .select('person_id', 'macrovisit_id', 'first_COVID_hospitalization_start_date', 'first_COVID_hospitalization_end_date')
        .dropDuplicates()
    )

    """
    ================================================================================
    If get_er_and_hosp_visits == True, include ER and hospital visits, otherwise 
       only include hospital visits.
    ================================================================================    
    """
    if get_er_and_hosp_visits == True:
        first_visits_df = first_hosp_df.join(first_er_df, 'person_id', 'outer')
    else:
        first_visits_df = first_hosp_df
         
             
    # Join in person facts
    pf_first_visits_df = pf_df.join(first_visits_df, 'person_id', 'left')    
  

  
    return pf_first_visits_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b0eef012-2041-4a78-9289-610f2661f10a"),
    death=Input(rid="ri.foundry.main.dataset.d8cc2ad4-215e-4b5d-bc80-80ffb3454875"),
    pf_first_covid_hosp=Input(rid="ri.foundry.main.dataset.c4d2279d-88e2-4360-90f2-43df60f1961f")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-12

Description:
Joins in death records. 

Note: 
Some patients have multiple death records, each with different causes of 
death. In these circumstances, and in the event there are conflicting
death_date values, the latest death_date value is chosen. 

Input:

Output:
================================================================================
"""
def pf_death(pf_first_covid_hosp, death):

    #  Subset columns
    deaths_df = death.select('person_id', 'death_date')

    pf_deaths = (
        pf_first_covid_hosp
        .select('person_id')
        .join(deaths_df, 'person_id', 'inner')
        .dropDuplicates()
    )
    
    # Order by macrovisit_id, poslab_date
    w = (
        Window
        .partitionBy('person_id')
        .orderBy('death_date')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    # In the event that there are multiple death_date values, the latest death_date is chosen
    pf_last_death_date = (
        pf_deaths
        .select('person_id'                                     ,
                F.last('death_date').over(w).alias('d_date')    )
        .withColumnRenamed('d_date', 'death_date')        
    )

    # Joins in patient data and creates death_recorded flag
    df = (
        pf_first_covid_hosp
        .join(pf_last_death_date, 'person_id', 'left')
        .withColumn('death_recorded', 
                    F.when(F.col('death_date').isNotNull(), F.lit(1))
                    .otherwise(0)
        )
    )

    return df

    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c4d2279d-88e2-4360-90f2-43df60f1961f"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905"),
    our_concept_sets=Input(rid="ri.foundry.main.dataset.f80a92e0-cdc4-48d9-b4b7-42e60d42d9e0"),
    pf_clean=Input(rid="ri.foundry.main.dataset.03e93e26-aa21-4f5d-b382-daaeea2a685e")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:
Adds hospitalization start and end dates and optionally Emergency Room visit 
dates. (To get both sets of dates, set get_er_and_hosp_visits == True)  
================================================================================ 
"""
def pf_first_covid_hosp( microvisit_to_macrovisit_lds, our_concept_sets, concept_set_members, pf_clean):

    """
    ================================================================================
    Potential Parameters 
    --------------------
    days_after_visit_start_date / days_before_visit_start_date (int)
    Proximity in days between index date(s) and visit date 
    ================================================================================ 
    """

    days_after_visit_start_date         = 1
    days_before_visit_start_date        = 16

    # Subset to macrovisits only (hospitalizations)
    hospitalizations_only_df = (
        microvisit_to_macrovisit_lds
        .select('person_id', 'macrovisit_id', 'macrovisit_start_date', 'macrovisit_end_date')
        .where(F.col('macrovisit_id').isNotNull())
    ).dropDuplicates()

    # only need these columns for join
    pf_poslab_df = pf_clean.select('person_id', 'first_pos_pcr_antigen_date')

    # COVID-associated hospitalizations
    all_poslab_hosp_df = (
        hospitalizations_only_df
        .join(pf_poslab_df, 'person_id', 'left')
        .where( (F.col('macrovisit_start_date')         >= F.col('first_pos_pcr_antigen_date') - days_after_visit_start_date) & 
                (F.col('macrovisit_start_date')         <= F.col('first_pos_pcr_antigen_date') + days_before_visit_start_date ) &
                (F.col('first_pos_pcr_antigen_date')    <= F.col('macrovisit_end_date')                         ) )
    )    
  

    # Order by person_id, macrovisit_start_date
    w = Window.partitionBy('person_id').orderBy('macrovisit_start_date')

    # Keep only first covid-associated hospitalization within range of first_pos_pcr_antigen_date
    first_hosp_per_poslab_df = (
        all_poslab_hosp_df
        .select('person_id'                                                     ,
                F.first('macrovisit_id').over(w).alias('md_id')                 ,
                'first_pos_pcr_antigen_date'                                    ,
                F.first('macrovisit_start_date').over(w).alias('start_date')    ,
                F.first('macrovisit_end_date'  ).over(w).alias('end_date')      )
        .dropDuplicates()
        .withColumnRenamed('md_id'      , 'first_covid_macrovisit_id'           )        
        .withColumnRenamed('start_date' , 'first_covid_macrovisit_start_date'   )
        .withColumnRenamed('end_date'   , 'first_covid_macrovisit_end_date'     )        
        #.withColumn('poslab_start_diff' ,  F.datediff(F.col('macrovisit_start_date'), F.col('first_pos_pcr_antigen_date')  ) )
        #.withColumn('end_start_diff'    ,  F.datediff(F.col('macrovisit_end_date'), F.col('macrovisit_start_date')         ) )        
    )

    # Join  
    df = (
        pf_clean
        .join(first_hosp_per_poslab_df.drop('first_pos_pcr_antigen_date'), 'person_id', 'left')
        .withColumn('covid_associated_hosp', 
                    F.when(F.col('first_covid_macrovisit_id').isNotNull(), F.lit(1) )
                    .otherwise(0))
    )

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55"),
    location=Input(rid="ri.foundry.main.dataset.efac41e8-cc64-49bf-9007-d7e22a088318"),
    manifest=Input(rid="ri.foundry.main.dataset.b1e99f7f-5dcd-4503-985a-bbb28edc8f6f"),
    person_lds=Input(rid="ri.foundry.main.dataset.50cae11a-4afb-457d-99d4-55b4bc2cbe66"),
    pf_sample=Input(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:
Adds in all person columns
Gets person address info from location
Gets treating institutions from manifest 

Input:

Output:
================================================================================
"""

def pf_locations(pf_sample, location, manifest, person_lds):

    # Adds patient data from person_lds
    with_person_df = (
        pf_sample
        .join(person_lds.select('person_id','year_of_birth','month_of_birth','day_of_birth',
                                  'ethnicity_concept_name','race_concept_name','gender_concept_name',
                                  'location_id','data_partner_id'),
              pf_sample.person_id == person_lds.person_id,
              how = "left"
        ).drop(person_lds.person_id)  
    )
    

    # Adds patient location data
    with_location_df = (
        with_person_df.join(
            location.select('location_id','city','state','zip','county'),
            with_person_df.location_id == location.location_id,
            how = "left"    
        ).drop(location.location_id)
    )

    # Adds contributing institution data 
    with_manifest_df = (
        with_location_df.join(
            manifest.select('data_partner_id','run_date','cdm_name','cdm_version','shift_date_yn','max_num_shift_days'),
            with_location_df.data_partner_id == manifest.data_partner_id,
            how = "left" 
        ).drop(manifest.data_partner_id)

    )

    return with_manifest_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57d6f26d-f01a-454d-bb1c-93408d9fdd51"),
    ALL_COVID_POS_PATIENTS=Input(rid="ri.foundry.main.dataset.d0f01e74-1ebb-46a5-b077-11864f9dd903")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-06-01

Description:

Input:

Output:
================================================================================
"""
def pf_sample(ALL_COVID_POS_PATIENTS):

    proportion_of_patients_to_use = 1.

    return ALL_COVID_POS_PATIENTS.sample(False, proportion_of_patients_to_use, 111)
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7ab2ef61-af7e-4a90-8f57-0cf330af8afe"),
    complete_patient_table_with_derived_scores=Input(rid="ri.foundry.main.dataset.d467585c-53b3-4f10-b09e-8e86a4796a1a"),
    pf_death=Input(rid="ri.foundry.main.dataset.b0eef012-2041-4a78-9289-610f2661f10a")
)
"""
================================================================================
Author: Elliott Fisher (elliott.fisher@duke.edu)
Date:   2022-08-09

Description:
This transform is considered obsolete due to its use of the use of the 
complete_patient_table_with_derived_scores. It is being kept in this code
workbook for reference.

Joins in severity scores associated with every patient in the Enclave. The 
severity score (Severity_Type) is linked to one "critical visit"      

Input:
1.  complete_patient_table_with_derived_scores 
    Created by the Bennet et. al. team
    See documentation here:
    https://unite.nih.gov/workspace/module/view/latest/ri.workshop.main.module.3ab34203-d7f3-482e-adbd-f4113bfd1a2b?id=KO-68B1026&view=focus

2.  pf_death
    Prior table in workbook
================================================================================
"""
def pf_severity(pf_death, complete_patient_table_with_derived_scores):

    severity_df = (
        complete_patient_table_with_derived_scores
        .withColumnRenamed('data_partner_id'    , 'sev_data_partner_id'     )
        .withColumnRenamed('visit_start_date'   , 'sev_visit_start_date'    )    
        .withColumnRenamed('visit_occurrence_id', 'sev_visit_occurrence_id' )
        .select('person_id'                 ,
                'Severity_Type'             ,
                'sev_data_partner_id'       ,
                'sev_visit_start_date'      ,
                'sev_visit_occurrence_id'   ,                   
                'AKI_in_hospital'           ,
                'ECMO'                      ,
                'Invasive_Ventilation'      ,
                'Q_Score'                   ,
                'BMI'                       ,
                'Height'                    ,
                'Weight'                    ,
                'blood_type'                ,
                'smoking_status'            
            )          
    )    

    pf_severity_df = pf_death.join(severity_df, 'person_id', 'left')                   
    

    return pf_severity_df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bccdc3d7-e19c-4b15-aeef-9f33623cbac0"),
    microvisit_to_macrovisit_lds=Input(rid="ri.foundry.main.dataset.5af2c604-51e0-4afa-b1ae-1e5fa2f4b905")
)
def successive_macrovisits(microvisit_to_macrovisit_lds):

    df = (
        microvisit_to_macrovisit_lds
        .select(
            'person_id',
            'macrovisit_id', 
            'visit_occurrence_id', 
            'visit_concept_id',
            'visit_concept_name', 
            'visit_start_date', 
            'visit_end_date',
            'macrovisit_start_date',
            'macrovisit_end_date'
        )
        .filter(F.col('macrovisit_id').isNotNull())
        .filter(F.col('person_id') == "5506278855900148501")
    ).sort('macrovisit_start_date')

    return df
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.de154c87-9a88-464d-a4f5-3147f70e55ab"),
    COVID_POS_PERSON_FACT=Input(rid="ri.foundry.main.dataset.97993cef-0004-43d1-9455-b28322562810")
)
def unique_zip_count(COVID_POS_PERSON_FACT):

    df = (
        COVID_POS_PERSON_FACT
        .groupBy('zip_code')
        .count()
        #.where(F.col('count') >= 15)
    )

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8e76946-4208-4982-9ecb-ef9e236247c3"),
    pf_locations=Input(rid="ri.foundry.main.dataset.628bfd8f-3d3c-4afb-b840-0daf4c07ac55")
)
def unique_zip_count_dirty(pf_locations):
    return pf_locations.groupBy('zip').count()    
    

