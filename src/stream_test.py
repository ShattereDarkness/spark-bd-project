from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer, HashingVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.metrics import confusion_matrix

import json
import re
import numpy as np


# Initialize the spark context.
sc = SparkContext(appName="SpamStreaming")
ssc = StreamingContext(sc, 5)

spark = SparkSession(sc)

schema = StructType([StructField("feature0", StringType(), True), StructField("feature1", StringType(), True), StructField("feature2", StringType(), True)])

vectorizer = HashingVectorizer()
le = LabelEncoder()
mnb = MultinomialNB()
data = None
batch_size = 1500

def removeNonAlphabets(s):
    s.lower()
    regex = re.compile('[^a-z\s]')
    s = regex.sub('', s)   
    return s

def vectorizeDstream(rdd):
    global data, X

    l = rdd.collect()

    if len(l):
        if data is None:
            # Data is a list of strings
            data = []
        
        # Create dataframe for rdd
        df = spark.createDataFrame(json.loads(rdd.collect()[0]).values(), schema)
        

        for x in df.collect():
            data.append(removeNonAlphabets(x['feature0'] + ' ' + x['feature1']))
        


        if len(data) == batch_size:
            # CountVectorize data
            X = vectorizer.partial_fit(np.array(data)).toarray()

            # Reinitialize data
            data = None
        
        return None

def func(rdd):
    global X

    l = rdd.collect()

    if len(l):
        df = spark.createDataFrame(json.loads(l[0]).values(), schema)

        # remove_non_alpha = udf(removeNonAlphabets, StringType())
        # new_df_0 = df.withColumn("feature0", remove_non_alpha(df["feature0"]))
        # new_df_1 = new_df_0.withColumn("feature1", remove_non_alpha(new_df_0["feature1"]))

        df_list = df.collect()

        # X = vectorizer.fit_transform(np.array([x['feature1']  for x in df_list]))

        y = le.fit_transform(np.array([x['feature2']  for x in df_list]))

        print("X:", X.shape)
        print("y:", y.shape)

        X_train, X_test, y_train, y_test = train_test_split(X, y,  random_state = 9)

        model = mnb.partial_fit(X_train, y_train, classes = np.unique(y_train))
        pred = model.predict(X_test)

        accuracy = accuracy_score(y_test, pred)
        precision = precision_score(y_test, pred)
        recall = recall_score(y_test, pred)
        conf_m = confusion_matrix(y_test, pred)

        print(f"accuracy: %.3f" %accuracy)
        print(f"precision: %.3f" %precision)
        print(f"recall: %.3f" %recall)
        print(f"confusion matrix: ")
        print(conf_m)


lines = ssc.socketTextStream("localhost", 6100)

lines.foreachRDD(vectorizeDstream)
lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()