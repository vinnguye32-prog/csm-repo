import os 
from datetime import datetime ,timedelta 
#import findspark
#findspark.init()
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.window import Window

spark = SparkSession.builder.config("spark.driver.memory", "8g").getOrCreate()

def read_data_from_path(path):
	df = spark.read.json(path)
	return df 

def select_fields(df):	
	df = df.select("_source.*")
	return df 
	
def calculate_devices(df):
	total_devices = df.select("Contract","Mac").groupBy("Contract").count()
	total_devices = total_devices.withColumnRenamed('count','TotalDevices')
	return total_devices
	
def transform_category(df):
    df = df.withColumn(
        "Type",
        when((col("AppName") == 'CHANNEL') | (col("AppName") == 'DSHD') | (col("AppName") == 'KPLUS') | (col("AppName") == 'KPlus'), "TV_Channel")
        .when((col("AppName") == 'VOD') | (col("AppName") == 'FIMS_RES') | (col("AppName") == 'BHD_RES') |
              (col("AppName") == 'VOD_RES') | (col("AppName") == 'FIMS') | (col("AppName") == 'BHD') | (col("AppName") == 'DANET'), "Movies")
        .when((col("AppName") == 'RELAX'), "Entertainment")
        .when((col("AppName") == 'CHILD'), "Kids")
        .when((col("AppName") == 'SPORT'), "Sports")
        .otherwise("Error")
    )
    return df 

def etl_most_watch(df):
    temp = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
    temp = temp.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    window = Window.partitionBy("Contract").orderBy(col("TotalDuration").desc())
    ranked = temp.withColumn("rank", row_number().over(window)).orderBy("Contract") #rank each contract by Total Duration
    # most_watched logic
    most_watched = ranked.filter(col("rank")==1).select("Contract","Type").withColumnRenamed("Type","Most_Watch")
    return most_watched

def etl_customer_taste(df):
    temp = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
    temp = temp.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    customer_taste = temp.filter(col("TotalDuration") >0).select("Contract","Type")
    customer_taste = customer_taste.groupBy("Contract").agg(concat_ws(",",sort_array(collect_list('Type'))).alias('customer_taste'))
    return customer_taste
    
def etl_active_days(df):
    df = df.groupBy("Contract").agg(count_distinct(col("Contract")).alias("Active"))
    return df

def active_agg(df):
    active_days = df.groupBy('Contract').sum().withColumnRenamed('sum(Active)','Active_Days')
    return active_days

def calculate_statistics(df): 
    statistics = df.select('Contract','TotalDuration','Type').groupBy('Contract','Type').sum()
    statistics = statistics.withColumnRenamed('sum(TotalDuration)','TotalDuration')
    statistics = statistics.groupBy('Contract').pivot('Type').sum('TotalDuration').na.fill(0)
    return statistics 
	
def finalize_result(statistics,total_devices,most_watched,customer_taste,active_days):
    result = statistics.join(total_devices,'Contract','inner')
    result = result.join(most_watched,'Contract','inner')
    result = result.join(customer_taste,'Contract','inner')
    result = result.join(active_days,'Contract','inner')
    result = result.drop("Error").filter((~col("Contract").isin('0','')) & (col("Contract").isNotNull()))
    
    return result 
	
# def save_data(result,save_path):
# 	result.repartition(1).write.mode("overwrite").option("header","true").csv(save_path)
# 	return print("Data Saved Successfully")

def etl_main(df,df_active):
    #df = select_fields(df)
    print('-------------Calculating Devices --------------')
    total_devices = calculate_devices(df)
    print('-------------Transforming Category --------------')
    df = transform_category(df)
    print('-------------Calculating Most Watched --------------')
    most_watched = etl_most_watch(df)
    print('-------------Calculating Customer Taste --------------')
    customer_taste = etl_customer_taste(df)
    print('-------------Calculating Customer Active Days --------------')
    active_days = active_agg(df_active)
    print('-------------Calculating Statistics --------------')
    statistics = calculate_statistics(df)
    print('-------------Finalizing result --------------')
    result = finalize_result(statistics,total_devices,most_watched,customer_taste,active_days)
    print(result)
    print('-------------Saving Results --------------')
    print('-----------------------------')
    #save_data(result,save_path)
    print("Finished job")
    return result

# ---- write to Databricks Delta table (monthly fact) ----
CATALOG = "workspace"
SCHEMA  = "csm_project"
TABLE   = "fact_monthly_customer_interaction"
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

#def input_path():
#    url = str(input('Please provide datadata source folder'))
#    return url

#def output_path():
#    url = str(input('Please provide destination folder'))
#    return url


### Combining JSON 
def list_files(path):
    # list_files = os.listdir(input_path)
    # print(list_files)
    # print("How many files you want to ETL")
    return [f.path for f in dbutils.fs.ls(path)]

input_path = 's3a://vinh-de-pipeline/bronze/log_content/'
#output_path = '/Users/vinnymac/Documents/Dev/Apache-Spark/Spark/DE Project/output'

list_files = list_files(input_path) 
print(list_files)

#start_date = str(input('Please input start_date format yyyymmdd'))
start_date = '20220401'
start_date = datetime.strptime(start_date,"%Y%m%d").date()
#to_date = str(input("Please input to_date format yyyymmdd"))
to_date = '20220430'
to_date = datetime.strptime(to_date,"%Y%m%d").date()

date_list = []
current_date = start_date 
end_date = to_date
while (current_date <= end_date):
    date_list.append(current_date.strftime("%Y%m%d"))
    current_date += timedelta(days=1)
print(date_list) 

start_time = datetime.now()
df = spark.read.json(input_path+date_list[0]+'.json')
df = select_fields(df)
df_active = etl_active_days(df)
for i in date_list[1:]:
    print("ETL_TASK" + input_path + i + ".json")
    new_df = spark.read.json(input_path +i + '.json')
    new_df = select_fields(new_df)
    # Calculate Active Contract for each file
    new_dfa = etl_active_days(new_df)
    print("Union df with new df")
    df = df.union(new_df)
    print("Union Daily Active Contract")
    df_active = df_active.union(new_dfa)

print("Calculation on final output")
result = etl_main(df,df_active)

month_yyyymm = start_date.strftime("%Y%m")
result = result.withColumn("month_yyyymm", lit(month_yyyymm))
#save_data(result,output_path)
save_fact_table(result, TARGET_TABLE)
end_time = datetime.now()
result.printSchema()
print((end_time - start_time).total_seconds())

