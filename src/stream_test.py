from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, udf, struct
from pyspark.ml.feature import Tokenizer, StopWordsRemover, StringIndexer
from pyspark.ml import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import LabelEncoder
import json
import re


# Initialize the spark context.
sc = SparkContext(appName="ScamStreaming")
ssc = StreamingContext(sc, 5)

spark = SparkSession(sc)

schema = StructType([StructField("feature0", StringType(), True), StructField("feature1", StringType(), True), StructField("feature2", StringType(), True)])

def removeNonAlphaNumeric(s):
    regex = re.compile('[^a-zA-Z\s]')
    s = regex.sub('', s)   
    return s

def func(rdd):
    l = rdd.collect()

    if len(l):
        df = spark.createDataFrame(json.loads(rdd.collect()[0]).values(), schema)

        remove_alpha = udf(removeNonAlphaNumeric, StringType())
        new_df = df.withColumn("feature0", remove_alpha(df["feature0"]))
        new_df.select('feature0').show()

        # df.show()
        
        # tokenizer = Tokenizer(inputCol="feature0", outputCol="words") 

        # stopremove = StopWordsRemover(inputCol='words',outputCol='stop_tokens')

        # data_prep_pipe = Pipeline(stages=[tokenizer, stopremove])
        # cleaner = data_prep_pipe.fit(df)
        # clean_data = cleaner.transform(df)

        # df.select('feature0').show()
        # new_data = df.collect() #[[]]
        # l = []
        # for row in new_data:
        #     l.append(row['feature0']) #['sayo', 'sucks']

        # vectorizer = CountVectorizer()
        # X = vectorizer.fit_transform(l)
        # X_arr =  X.toarray()

        # le = LabelEncoder()
        # y = le.fit(new_data["feature2"])


lines = ssc.socketTextStream("localhost", 6100)
lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()