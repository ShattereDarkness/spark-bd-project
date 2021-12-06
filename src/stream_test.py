from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import Perceptron, SGDClassifier
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix, f1_score

import json
import re
import numpy as np

TRAIN_SIZE = 20
TEST_SIZE = 2
BATCH_SIZE = TRAIN_SIZE + TEST_SIZE

# Initialize the spark context.
sc = SparkContext(appName="SpamStreaming")
ssc = StreamingContext(sc, 5)

spark = SparkSession(sc)

schema = StructType([StructField("feature0", StringType(), True), StructField("feature1", StringType(), True), StructField("feature2", StringType(), True)])

vectorizer = HashingVectorizer(alternate_sign=False)
le = LabelEncoder()
mnb = MultinomialNB()
sgd = SGDClassifier(warm_start=True)
per = Perceptron(warm_start=True)
kmeans = MiniBatchKMeans(n_clusters=2)
N = 0
classes = []

def removeNonAlphabets(s):

    s.lower()
    regex = re.compile('[^a-z\s]')
    s = regex.sub('', s)   
    return s

def print_stats(y, pred):

    accuracy = accuracy_score(y, pred)
    precision = precision_score(y, pred)
    recall = recall_score(y, pred)
    conf_m = confusion_matrix(y, pred)
    f1 = f1_score(y, pred)

    print(f"\naccuracy: %.3f" %accuracy)
    print(f"precision: %.3f" %precision)
    print(f"recall: %.3f" %recall)
    print(f"f1-score : %.3f" %f1)
    print(f"confusion matrix: ")
    print(conf_m)

def func(rdd):
    global N
    global classes
    l = rdd.collect()

    if len(l):
        df = spark.createDataFrame(json.loads(l[0]).values(), schema)

        df_list = df.collect()

        if N < TRAIN_SIZE:
            X_train = vectorizer.fit_transform([(removeNonAlphabets(x['feature0'] + ' ' + x['feature1'])) for x in df_list])

            y_train = le.fit_transform(np.array([x['feature2']  for x in df_list]))
            
            if N == 0:
                classes = np.unique(y_train)

            #multinomial nb
            mnb.partial_fit(X_train, y_train, classes = classes)

            #perceptron
            per.partial_fit(X_train, y_train, classes = classes)

            #sgdclassifier
            sgd.partial_fit(X_train, y_train, classes = classes)

            #k means clustering
            kmeans.partial_fit(X_train, y_train)
            
        else:
            X_test = vectorizer.fit_transform([(removeNonAlphabets(x['feature0'] + ' ' + x['feature1'])) for x in df_list])

            y_test = le.fit_transform(np.array([x['feature2']  for x in df_list]))

            #multinomial nb
            pred = mnb.predict(X_test)
            print("\nMultinomial NB: ")
            print_stats(y_test, pred)

            #perceptron
            pred = per.predict(X_test)
            print("\nPerceptron: ")
            print_stats(y_test, pred)

            #sgdclassifier
            pred = sgd.predict(X_test)
            print("\nSGD Classifier: ")
            print_stats(y_test, pred)

            #k means clustering
            pred = kmeans.predict(X_test)
            print("\nK-Means: ")
            print_stats(y_test, pred)

        N += 1
        
        N = N % BATCH_SIZE



lines = ssc.socketTextStream("localhost", 6100)

lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()