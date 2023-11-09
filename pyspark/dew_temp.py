from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, avg, concat_ws

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
rawDf = spark.read.option("header", True).csv("./normal_hly_sample_temperature.csv")

relvelantColsDf = rawDf.select("DATE", "HLY-TEMP-NORMAL", "HLY-DEWP-NORMAL")

timeRemovedDf = relvelantColsDf.withColumn("DATE", substring("DATE", 1,8))

averagedDf = timeRemovedDf.groupBy("DATE") \
    .agg(avg("HLY-TEMP-NORMAL").alias("AVG_NORMAL_TEMP"), \
         avg("HLY-DEWP-NORMAL").alias("AVG_DEW_TEMP")).sort("DATE")

resultDf = averagedDf.select("DATE", concat_ws(", ", averagedDf.AVG_NORMAL_TEMP, averagedDf.AVG_DEW_TEMP).alias("RESULT"))

resultDf.write.option("sep","\t").csv("./pyspark/output")