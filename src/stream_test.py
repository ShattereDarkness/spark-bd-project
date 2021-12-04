from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json

# Initialize the spark context.
sc = SparkContext(appName="ScamStreaming")
ssc = StreamingContext(sc, 5)

spark = SparkSession(sc)

schema = StructType([StructField("feature0", StringType(), True), StructField("feature1", StringType(), True), StructField("feature2", StringType(), True)])

def func(rdd):
    if len(rdd.collect()):        
        df = spark.createDataFrame(json.loads(rdd.collect()[0]).values(), schema)
        df.select("feature0").show()

lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()