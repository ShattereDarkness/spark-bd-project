from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json

# Initialize the spark context.
sc = SparkContext(appName="ScamStreaming")
ssc = StreamingContext(sc, 5)

spark = SparkSession(sc)

def func(rdd):
    if len(rdd.collect()):        
        df = spark.createDataFrame(rdd)

lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()