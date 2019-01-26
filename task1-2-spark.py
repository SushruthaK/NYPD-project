from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from operator import add
import sys
import os
APP_NAME = "NYPD"

def main(spark,filename):
    #Performing cleaning of data and finding all the required outputs for task2 in same spark file
    #Selecting the required columns to perform our operations
    spark_df=spark.read.csv(filename,header=False);
    validated_df=spark_df.select("_c0","_c2","_c3","_c10","_c11","_c12","_c13","_c14","_c15","_c16","_c17","_c24")
    #Performing task-1 - cleaning the data by checking if the columns are null or not
    cleaned_data=validated_df.where("_c0 is not null and _c2 is not null and _c3 is not null and _c10 is not null and _c11 is not null and _c12 is not null and _c14 is  not null and _c15 is not null and _c16 is not null and _c17 is not null and _c24 is not null and  _c13 is not null")
    #Now store the output into table so that we can perform our analysis required for task2-naming the table as task1op
    cleaned_data.registerTempTable("task1op")
    #loading data to a file to be used as input to mapreduce
    tempcsv=spark.sql("select _c0,_c2,_c3,_c10,_c11,_c12,_c13,_c14,_c15,_c16,_c17,_c24 from task1op")
    tempcsv.write.format("csv").save("/user/kasiresa/resultsparktask1/")
    #Date on which maximum number of accidents took place
    task2a=spark.sql("select _c0,count(*) as total from task1op group by _c0 order by total desc  limit 1")
    task2a.write.format("csv").save("/user/kasiresa/resultspark/task2.1")
    #Borough with maximum count of accident fatality
    task2b=spark.sql("select _c2,sum(int(_c11+_c13+_c15+_c17)) as total from task1op group by _c2 order by total desc  limit 1")
    task2b.write.format("csv").save("/user/kasiresa/resultspark/task2.2")
	#Zip with maximum count of accident fatality
    task2c=spark.sql("select _c3,sum(int(_c11+_c13+_c15+_c17)) as total from task1op group by _c3 order by total desc  limit 1")
    task2c.write.format("csv").save("/user/kasiresa/resultspark/task2.3")
	#Which vehicle type is involved in maximum accidents
    task2d=spark.sql("select _c24,count(*) as total from task1op group by _c24 order by total desc  limit 1")
    task2d.write.format("csv").save("/user/kasiresa/resultspark/task2.4")
	#Year in which maximum Number Of Persons and Pedestrians Injured
    task2e=spark.sql("select year(to_date(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(_c12+_c10)) as total from task1op group by `Year` order by total desc limit 1")
    task2e.write.format("csv").save("/user/kasiresa/resultspark/task2.5")
	#Year in which maximum Number Of Persons and Pedestrians Killed
    task2f=spark.sql("select year(to_date(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(_c11+_c13)) as total from task1op group by `Year` order by total desc limit 1")
    task2f.write.format("csv").save("/user/kasiresa/resultspark/task2.6")
	#Year in which maximum Number Of Cyclist Injured and Killed (combined)
    task2g=spark.sql("select year(to_date(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(_c14+_c15)) as total from task1op group by `Year` order by total desc limit 1")
    task2g.write.format("csv").save("/user/kasiresa/resultspark/task2.7")
	#Year in which maximum Number Of Motorist Injured and Killed (combined)
    task2h=spark.sql("select year(to_date(cast(unix_timestamp(_c0, 'MM/dd/yyyy') as timestamp))) as Year,sum(int(_c16+_c17)) as total from task1op group by `Year` order by total desc limit 1")
    task2h.write.format("csv").save("/user/kasiresa/resultspark/task2.8")
    
          
if __name__ == "__main__":
   # Configure Spark
   spark=SparkSession.builder.master("local").appName(APP_NAME).getOrCreate()
   filename=sys.argv[1]
   # Execute Main functionality
   main(spark,filename)
  
   