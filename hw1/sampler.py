##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: Karan Shah

##Data Science Imports: 
import numpy as np
import mmh3
from random import random
from random import shuffle

##IO, Process Imports: 
import sys
from pprint import pprint


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    # Step 1: read file to pull out unique user_ids from file
    # Step 2: subset to random  1% of user_ids
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn

    mean, standard_deviation = 0.0, 0.0

    ##<<COMPLETE>>
    unique_ids = set()
    for line in filename:
      userid = line.split(',')[2]
      if userid not in unique_ids:
        unique_ids.add(userid)
    # subset = np.random.choice(list(unique_ids), int(percent*len(unique_ids)))
    shuffle(list(unique_ids))
    subset = list(unique_ids)[:int(percent*len(unique_ids))]
    subset = set(subset)
    filename.seek(0)
    res = []
    for line in filename:
      userid, amount = line.split(',')[2:4]
      if userid in subset:
        res.append(float(amount))
    mean = np.mean(res)
    standard_deviation = np.std(res)
    mean = round(mean, 4)
    standard_deviation = round(standard_deviation, 4)
    
    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    mean, standard_deviation = 0.0, 0.0
    ##<<COMPLETE>>
    hashmap = set()
    count = 0
    variance = 0
    a = percent
    b = 1
    while a < 1:
      a = a*10
      b = b *10

    bucket = int(a)
    for line in stream:
        ##<<COMPLETE>>
        linedata = line.split(',')
        key = linedata[sample_col]
        value = linedata[sample_col+1]
        hashedkey = mmh3.hash(key)
        if hashedkey not in hashmap:
          hashmap.add(hashedkey)
          if hashedkey %  b < bucket:
            count += 1
            delta = float(value) - mean
            mean += delta / count
            variance += (delta*(float(value) - mean))
    standard_deviation = (variance / count)**0.5
    mean = round(mean, 4)
    standard_deviation = round(standard_deviation, 4)
        # pass
    ##<<COMPLETE>>

    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv', 'transactions_large.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    import datetime
    outerstart = datetime.datetime.now()
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            start = datetime.datetime.now()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            Typicalduration = datetime.datetime.now() - start
            
            fstream.close()
            fstream = open(f, "r")
            start = datetime.datetime.now()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            Streamduration = datetime.datetime.now() - start
            print("Time taken by Typical Sampler: ", Typicalduration)
            print("Time taken by Stream Sampler : ", Streamduration)
    print("Total time:", datetime.datetime.now() - outerstart)


