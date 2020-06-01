import re
import json
import math
import numpy as np
from scipy import stats
from pyspark import SparkContext
from numpy.linalg import norm
import sys

args = sys.argv
sc = SparkContext()


productIDs = eval(args[2]) # target items

pathToJson = args[1] # path to input data
rdd = sc.textFile(pathToJson)


# first filter - per rating per user per item

def perUser(record):
  record = json.loads(record)
  user = record['reviewerID']
  item = record['asin']
  rating = record['overall']
  return ((user, item), (rating))

temp = rdd.map(lambda record: perUser(record)).reduceByKey(lambda a,b:b)

# second, third filter

filteredRDD = temp\
.map(lambda x: (x[0][1], [(x[0][0], x[1])]))\
.reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 24)\
.flatMapValues(lambda x:x).map(lambda x: (x[1][0], [(x[0], x[1][1])]))\
.reduceByKey(lambda x,y: x+y).filter(lambda x: len(x[1]) > 4)

usersRDD = filteredRDD.keys().distinct() # received total users after filtering

filteredRDD = filteredRDD.flatMapValues(lambda x:x).map(lambda x: ((x[1][0]), [(x[0], x[1][1])])).reduceByKey(lambda x,y : x+y) # changed format to key, list


# mean centered ratings

def changeRating(record):
  item = record[0]
  userRatingTuples = record[1]
  mean = np.mean([i for _,i in userRatingTuples])
  res = {x[0]:x[1]-mean for x in userRatingTuples}
  res2 = {x[0]:x[1] for x in userRatingTuples}
  return (item, (res, res2))
  
meanCentered_filteredRDD = filteredRDD.reduceByKey(lambda x,y: x+y).map(lambda x: changeRating(x))

# removing target data
targetRDD = meanCentered_filteredRDD.filter(lambda record: record[0] in productIDs)

# joining target data with mean centered data
cartRDD = targetRDD.cartesian(meanCentered_filteredRDD)

# cosine similarity
def findSimilarity(vec1, vec2, int_vec1, int_vec2):
    return np.dot(int_vec1, int_vec2)/(norm(vec1)*norm(vec2))
    

# finding similarity
def similarityCheck(record):
  targetRecord = record[0]
  currentRecord = record[1]
  intersection = currentRecord[1][0].keys() & targetRecord[1][0].keys()
  
  if len(intersection) < 2: # condition in assignment: the intersection of users_with_ratings for the two is < 2
    return False

  # tuples to dict
  currentDict = { key: currentRecord[1][0][key] for key in currentRecord[1][0] }
  targetDict = { key: targetRecord[1][0][key] for key in targetRecord[1][0] }

  # dict to list
  currentVector = [currentDict[i] for i in currentDict.keys()]
  targetVector = [targetDict[i] for i in targetDict.keys()]

  # intersected dict
  int_currentDict = { key: currentRecord[1][0][key] for key in intersection }
  int_targetDict = { key: targetRecord[1][0][key] for key in intersection }

  # intersected list
  int_currentVector = [int_currentDict[i] for i in intersection]
  int_targetVector = [int_targetDict[i] for i in intersection]

  sim = findSimilarity(targetVector, currentVector, int_targetVector, int_currentVector)
  
  # only if similarity > 0
  return (targetRecord[0], [(targetRecord[1][1], (currentRecord[0], sim, currentRecord[1][1]))]) if sim > 0 else False
  
simRDD = cartRDD.map(lambda x: similarityCheck(x)).filter(bool)

#total users broadcasted
usersRDD = sc.broadcast(usersRDD.collect())

# prediction
def utility(pred):
  return sum([i*j for i, j in pred]) / sum([i for i,_ in pred])

# lets predict/ create utility matrix
def predict(record):
  targetUsers = usersRDD.value - record[1][0][0].keys()
  predictions = {}
  for user in usersRDD.value:
    if user in record[1][0][0].keys(): # if rating already given
      predictions[user] = record[1][0][0][user]
    else: # lets predict
      pred = [(i[1][1], i[1][2][user]) for i in record[1] if user in i[1][2].keys()]
      if len(pred) < 2: # Within a target row, do not make predictions for columns (i.e. users) that do not have at least 2 neighbors with values
        continue
      predictions[user] = utility(pred)
  return (record[0], predictions)

result = simRDD.reduceByKey(lambda x, y: x + y).map(lambda x: predict(x))

print(result.collect())

# the end!