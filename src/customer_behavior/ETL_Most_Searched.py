import os 
from datetime import datetime ,timedelta 
from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.window import Window 
spark = SparkSession.builder.getOrCreate()

start_time = datetime.now()

start_date = '20220601'
start_date = datetime.strptime(start_date,"%Y%m%d").date()
end_date = '20220714'
end_date = datetime.strptime(end_date,"%Y%m%d").date()

def process_log_search_data(df):
    df = df.select('user_id','keyword')
    df = df.filter((col('user_id').isNotNull()) & (col('keyword').isNotNull())  )
    df = df.groupBy('user_id','keyword').count().withColumnRenamed('count','frequency')
    w = Window.partitionBy('user_id').orderBy(col('frequency').desc())
    df = df.withColumn("rank",row_number().over(w))
    df = df.filter(col('rank') == 1)
    df = df.select('user_id','keyword').withColumnRenamed('keyword','most_searched')
    df = df.orderBy('user_id',ascending = False )
    return df

def export_unlabelled_program(prev_program, curr_program, outputpath):
    unlabelled_program = prev_program.union(curr_program).select('most_searched').distinct()
    unlabelled_program.toPandas().to_csv(outputpath, index=False)
    print(f"{unlabelled_program.count()} Unlabelled Programs Saved to {outputpath}")

def map_genre(most_searched, mapping):
    df = most_searched.join(broadcast(mapping),
                             on = ["most_searched"], 
                             how="inner")
    #.withColumn("genre", coalesce(col("genre"), lit("Other")))
    return df

def trend_result(df_previous_month, df_current_month):
    prev = df_previous_month.alias("p")
    curr = df_current_month.alias("c")
    trend = prev.join(curr, on = ['user_id'],how='inner')\
                .withColumn(
                            "Trending_Type",
                            when(col("p.genre") == col("c.genre"), "Unchanged")
                            .otherwise("Changed"))\
                .withColumn("Previous_Genre", when(col("Trending_Type")=="Unchanged","Unchanged").otherwise(col("p.genre")))
    return trend

# trend = trend_result(mapped_06,mapped_07)
# trend.show(20)
# ---- write to Databricks Delta table (monthly fact) ----
CATALOG = "workspace"
SCHEMA  = "csm_project"
TABLE   = "fact_monthly_customer_behavior"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

def save_fact_table(df, target_table):
    (df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .partitionBy("month_yyyymm")
      .saveAsTable(target_table))
    print(f"Saved to {target_table}")

try:
    spark.sql(f"DELETE FROM {TARGET_TABLE} WHERE month_yyyymm = '{month_yyyymm}'")
except:
    pass  

def etl_main(df_previous_month, df_current_month):
    print('------------- Finding Most Searched Each Month...------------')
    print('-------------------------------------------------------------')
    ms_06 = process_log_search_data(df_previous_month)
    print('------------- Most Searched Previous Month: -------------')
    print('---------------------------------------------------------')
    ms_06.show(5)
    ms_07 = process_log_search_data(df_current_month)
    print('------------- Most Searched Current Month: --------------')
    print('---------------------------------------------------------')
    ms_07.show(5)
    print('------------- Export Unlabelled Program... --------')
    print('---------------------------------------------------')
    output_path = "/Workspace/Users/vinnguye32@gmail.com/csm-repo/src/customer_behavior/map/program_input.csv"
    #export_unlabelled_program(ms_06,ms_07,output_path)
    ### Now use ChatGPT to Label Movie_Input. File Name mapping.py uses OpenAI API to prompt and return the AI Labelled Data
    ## After saving our AI Labelled Data, we can use it to map our movie genre
    print('-----------------------------------------------------')
    print('------------- Get Program Genre Label... -------------')
    print('-----------------------------------------------------')
    mapping = spark.read.csv("/Workspace/Users/vinnguye32@gmail.com/csm-repo/src/customer_behavior/map/program_AI_Labelled_output.csv",header = True)
    mapping.show(5)
    print('------------- Mapping Programs with Genre... -----------')
    print('--------------------------------------------------------')
    mapped_06 = map_genre(ms_06,mapping)
    print('------------- Previous Month Mapping: -------------')
    print('---------------------------------------------------')
    mapped_06.show(5)
    mapped_07 = map_genre(ms_07,mapping)
    print('------------- Current Month Mapping: -------------')
    print('--------------------------------------------------')
    mapped_07.show(5)
    print('------------ Perform MoM Trend Analysis... -------')
    print('--------------------------------------------------')
    trend = trend_result(mapped_06,mapped_07)
    trend.show(20)
    print('------------ Saving result to Database... --------')
    print('--------------------------------------------------')
    month_yyyymm = end_date.strftime("%Y%m")
    trend = trend.withColumn("month_yyyymm", lit(month_yyyymm))
    trend = trend.select('user_id','Previous_Genre','Trending_Type','month_yyyymm')
    save_fact_table(trend, TARGET_TABLE)
    return trend

### Function to combine raw data files for a given month
def list_all_files(path):
    files = []
    for entry in dbutils.fs.ls(path):
        if entry.isDir():
            files.extend(list_all_files(entry.path))
        else:
            files.append(entry.path)
    return files

def combine_raw_data(input_path, month_pattern):
    all_files = list_all_files(input_path)
    month_files = [f for f in all_files if month_pattern in f and f.endswith('.parquet')]
    print(f"Files loaded for {month_pattern}:\n" + '\n'.join(month_files))
    df = None
    for f in month_files:
        if df is None:
            df = spark.read.parquet(f)
        else:
            df = df.union(spark.read.parquet(f))
    if df is None:
        df = spark.createDataFrame([], 'user_id string, keyword string')
    return df

### Read and combine all monthly files for trend analysis
input_path = 's3a://vinh-de-pipeline/bronze/log_search/'
print('------------- Reading Parquet Files --------------')
print('--------------------------------------------------')
df_06 = combine_raw_data(input_path, '202206')
df_07 = combine_raw_data(input_path, '202207')
result = etl_main(df_06,df_07)

end_time = datetime.now()
print((end_time - start_time).total_seconds())
