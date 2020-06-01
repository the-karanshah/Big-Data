import re
import json
import math
import numpy as np
from scipy import stats
from pyspark import SparkContext
import sys

args = sys.argv
sc = SparkContext()

pathToJson = args[1] # path to input data
rdd = sc.textFile(pathToJson)

def getWords(record):
    record = json.loads(record)
    try:
      review = record['reviewText'].lower()
    except:
      return []
    words = re.findall("((?:[.,!?;\"])|(?:(?:#|@)?[A-Za-z0-9_-]+(?:'[a-z]{1,3})?))", review)
    return words

mostCommonWords = rdd.flatMap(lambda record: getWords(record)).map(lambda word: (word,1)).reduceByKey(lambda x,y: x+y).sortBy(lambda x:x[1],False)

kwords = mostCommonWords.take(1000) # most common 1000 words

kwords = sc.broadcast(kwords) # broadcasted

def getRelativeFreq(record):
    record = json.loads(record)
    try:
      review = record['reviewText'].lower() # lower text
      if not review: return [] # if no review found
      words = re.findall("((?:[.,!?;\"])|(?:(?:#|@)?[A-Za-z0-9_-]+(?:'[a-z]{1,3})?))", review)
      if not words: return [] # if no words in review
      ratings = record['overall']
      verified  = int(record['verified']) # boolean to int
    except:
      return []    # if error
    totalcount = len(words)
    results = []
    for (word, _) in kwords.value: # most common 1000 words
      wordcount = review.count(word) # word count in this review
      # if wordcount:
      relativeFreq = wordcount / totalcount
      res = (word, [(relativeFreq, verified, ratings)])
      results.append(res)
    return results

relativeFreqWords = rdd.flatMap(lambda record: getRelativeFreq(record)).reduceByKey(lambda x,y: x+y) 

def computeCorrelation(record, control = True):
    word = record[0]
    details = record[1]

    x_1 = np.array([0.1]*len(details)) # empty arrays with float datatype
    x_2 = np.array([0.1]*len(details))
    y = np.array([0.1]*len(details))

    for i, detail in enumerate(details):
        x_1[i], x_2[i], y[i] = detail # tuple extraction

    if sum(x_2) == 0:
        x_2[-1] = 1 # for autocad, all verified has 0, resulting into nan

    if control:
        print("With control")
        x = np.concatenate([x_1[..., np.newaxis], x_2[..., np.newaxis]], axis = 1)
    else:
        print("Without control")
        x = x_1[..., np.newaxis]
  
    x_norm = (x - np.mean(x, axis=0))/ np.std(x, axis=0) # standardization

    y_norm = (y - np.mean(y))/np.std(y) # standardization

    ones = np.ones(x.shape[0])

    x_mat = np.concatenate([ones[...,np.newaxis], x_norm], axis=1) # adding one vector

    y_mat = np.array(y_norm)[:,np.newaxis]

    beta = np.linalg.inv(x_mat.T.dot(x_mat)).dot(x_mat.T).dot(y_mat) # beta from matrix multiplication

    y_pred = np.array(x_mat.dot(beta)) # prediction

    df = len(x_mat) - x.shape[-1] - 1 # degree of freedom, N-3 if controlled else N-2

    rss = np.sum(np.square(y_norm - y_pred))
    s_square = rss / df

    x_diff = np.sum(np.square(x_mat[:,1]))

    se = math.sqrt(s_square / x_diff)
    t = beta[1] / se
  
    pval = stats.t.sf(np.abs(t), df)
    if pval > 0.5:
        pval = (1 - pval)*2
    else:
        pval = pval*2
    pval_corrected = pval[0]*1000 # bonferroni
    return (word, beta[1], pval_corrected)

print("About to start the fun")
finalRes = relativeFreqWords.map(lambda record: computeCorrelation(record, True))
col = finalRes.sortBy(lambda x: x[1], False).collect()
print("With Control Top positively Correlated: ", col[:20])
print("With Control Top negatively Correlated: ", col[-20:])
finalRes = relativeFreqWords.map(lambda record: computeCorrelation(record, False))
col = finalRes.sortBy(lambda x: x[1], False).collect()
print("Without Control Top positively Correlated: ", col[:20])
print("Without Control Top negatively Correlated: ", col[-20:])