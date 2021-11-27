from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext

# Initialize the spark context.
sc = SparkContext(appName="ScamStreaming")
ssc = StreamingContext(sc, 1)

lines = ssc.socketTextStream("localhost", 6100)
lines.pprint()

ssc.start()
ssc.awaitTermination()
