from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf

from sklearn.model_selection import learning_curve
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.preprocessing import LabelEncoder
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import Perceptron, SGDClassifier
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import accuracy_score, precision_score, recall_score, confusion_matrix, f1_score, classification_report

import json
import re
import numpy as np
import time
import argparse

parser = argparse.ArgumentParser(
    description='Streams a file to a Spark Streaming Context')
parser.add_argument('--batch-size', '-b', help='Batch size',
                    required=False, type=int, default=100)

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
d = dict()

args = parser.parse_args()

TRAIN_SIZE = int(30345/args.batch_size)
TEST_SIZE = int(3373/args.batch_size)
BATCH_SIZE = TRAIN_SIZE + TEST_SIZE

def removeNonAlphabets(s):

    s.lower()
    regex = re.compile('[^a-z\s]')
    s = regex.sub('', s)   
    return s

def print_stats(y, pred):

    # global classes

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

    print(classification_report(y, pred, labels = classes))

def func(rdd):
    global N, d, classes

    l = rdd.collect()

    if len(l):
        df = spark.createDataFrame(json.loads(l[0]).values(), schema)

        df_list = df.collect()

        if N < TRAIN_SIZE:
            X_train = vectorizer.fit_transform([(removeNonAlphabets(x['feature0'] + ' ' + x['feature1'])) for x in df_list])

            y_train = le.fit_transform(np.array([x['feature2']  for x in df_list]))
            
            if N == 0:                
                print(args.batch_size)
                classes = np.unique(y_train)
                d['mnb'] = 0
                d['per'] = 0
                d['sgd'] = 0
                d['kmeans'] = 0

            #multinomial nb
            start_time = time.time()
            mnb.partial_fit(X_train, y_train, classes = classes)
            d['mnb'] += time.time() - start_time

            #perceptron
            start_time = time.time()
            per.partial_fit(X_train, y_train, classes = classes)
            d['per'] += time.time() - start_time

            #sgdclassifier
            start_time = time.time()
            sgd.partial_fit(X_train, y_train, classes = classes)
            d['sgd'] += time.time() - start_time

            #k means clustering
            start_time = time.time()
            kmeans.partial_fit(X_train, y_train)
            d['kmeans'] += time.time() - start_time
            
        else:
            X_test = vectorizer.fit_transform([(removeNonAlphabets(x['feature0'] + ' ' + x['feature1'])) for x in df_list])

            y_test = le.fit_transform(np.array([x['feature2']  for x in df_list]))

            #multinomial nb
            start_time = time.time()
            pred = mnb.predict(X_test)
            d['mnb'] += time.time() - start_time
            print("\nMultinomial NB: ")
            print_stats(y_test, pred)

            #perceptron
            start_time = time.time()
            pred = per.predict(X_test)
            d['per'] += time.time() - start_time
            print("\nPerceptron: ")
            print_stats(y_test, pred)

            #sgdclassifier
            start_time = time.time()
            pred = sgd.predict(X_test)
            d['sgd'] += time.time() - start_time
            print("\nSGD Classifier: ")
            print_stats(y_test, pred)

            #k means clustering
            start_time = time.time()
            pred = kmeans.predict(X_test)
            d['kmeans'] += time.time() - start_time
            print("\nK-Means: ")
            print_stats(y_test, pred)

        N += 1
        
        if N == BATCH_SIZE:
            for k, v in d.items():
                print(k, end = " ")
                print(f"time : %.3f secs" %v)

        N = N % BATCH_SIZE


lines = ssc.socketTextStream("localhost", 6100)

lines.foreachRDD(func)

ssc.start()
ssc.awaitTermination()
ssc.stop()

#batch_size on x axis time on y axis