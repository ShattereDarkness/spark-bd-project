from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import json

# Initialize the spark context.
sc = SparkContext(appName="ScamStreaming")
ssc = StreamingContext(sc, 1)

spark = SparkSession(sc)

schema = ['feature0' , 'feature1', 'feature2']

def func(rdd):
    if len(rdd.collect()):
        df = spark.createDataFrame(json.loads(rdd.collect()[0]).values() , schema = schema)
        print(df)

lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()