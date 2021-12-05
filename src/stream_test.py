from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import LabelEncoder
import json
import re
import numpy as np


# Initialize the spark context.
sc = SparkContext(appName="SpamStreaming")
ssc = StreamingContext(sc, 5)

spark = SparkSession(sc)

schema = StructType([StructField("feature0", StringType(), True), StructField("feature1", StringType(), True), StructField("feature2", StringType(), True)])

vectorizer = CountVectorizer()
le = LabelEncoder()

def removeNonAlphabets(s):
    s.lower()
    regex = re.compile('[^a-z\s]')
    s = regex.sub('', s)   
    return s

def func(rdd):
    l = rdd.collect()

    if len(l):
        df = spark.createDataFrame(json.loads(rdd.collect()[0]).values(), schema)

        remove_non_alpha = udf(removeNonAlphabets, StringType())
        new_df_0 = df.withColumn("feature0", remove_non_alpha(df["feature0"]))
        new_df_1 = new_df_0.withColumn("feature1", remove_non_alpha(new_df_0["feature1"]))

        X = vectorizer.fit_transform(np.array(new_df_1.select(['feature1']).rdd.map(lambda r: r[0]).collect())).toarray()
        y = le.fit_transform(np.array(new_df_1.select('feature2').rdd.map(lambda r: r[0]).collect()))

        print(X, y)


lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()