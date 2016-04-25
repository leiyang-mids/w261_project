
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import SparseVector 
from collections import defaultdict
from datetime import datetime
from sklearn import metrics
from math import log, exp
import numpy as np
import hashlib

# hash function
def hashFunction(numBuckets, rawFeats, printMapping=False):
    """Calculate a feature dictionary for an observation's features based on hashing.

    Note:
        Use printMapping=True for debug purposes and to better understand how the hashing works.

    Args:
        numBuckets (int): Number of buckets to use as features.
        rawFeats (list of (int, str)): A list of features for an observation.  Represented as
            (featureID, value) tuples.
        printMapping (bool, optional): If true, the mappings of featureString to index will be
            printed.

    Returns:
        dict of int to float:  The keys will be integers which represent the buckets that the
            features have been hashed to.  The value for a given key will contain the count of the
            (featureID, value) tuples that have hashed to that key.
    """
    mapping = {}
    for ind, category in rawFeats:
        featureString = category + str(ind)
        mapping[featureString] = int(int(hashlib.md5(featureString).hexdigest(), 16) % numBuckets)
    if(printMapping): print mapping
    sparseFeatures = defaultdict(float)
    for bucket in mapping.values():
        sparseFeatures[bucket] += 1.0
    return dict(sparseFeatures)

# feature hash
def parseHashPoint(point, numBuckets):
    """Create a LabeledPoint for this observation using hashing.

    Args:
        point (str): A comma separated string where the first value is the label and the rest are
            features.
        numBuckets: The number of buckets to hash to.

    Returns:
        LabeledPoint: A LabeledPoint with a label (0.0 or 1.0) and a SparseVector of hashed
            features.
    """    
    elem = point.strip().split(',')    
    rawFea = [(i, elem[i+1]) for i in range(len(elem) - 1)]
    index = np.sort(hashFunction(numBuckets, rawFea, False).keys())    
    return LabeledPoint(elem[0], SparseVector(numBuckets, index, [1]*len(index)))   

# Logistic Regression Modeling & Evaluation
def getP(x, w, intercept):
    """Calculate the probability for an observation given a set of weights and intercept.

    Note:
        We'll bound our raw prediction between 20 and -20 for numerical purposes.

    Args:
        x (SparseVector): A vector with values of 1.0 for features that exist in this
            observation and 0.0 otherwise.
        w (DenseVector): A vector of weights (betas) for the model.
        intercept (float): The model's intercept.

    Returns:
        float: A probability between 0 and 1.
    """
    rawPrediction = x.dot(w) + intercept
    # Bound the raw prediction value
    rawPrediction = min(rawPrediction, 20)
    rawPrediction = max(rawPrediction, -20)
    return 1/(1+exp(-rawPrediction))


def computeLogLoss(p, y):
    """Calculates the value of log loss for a given probabilty and label.

    Note:
        log(0) is undefined, so when p is 0 we need to add a small value (epsilon) to it
        and when p is 1 we need to subtract a small value (epsilon) from it.

    Args:
        p (float): A probabilty between 0 and 1.
        y (int): A label.  Takes on the values 0 and 1.

    Returns:
        float: The log loss value.
    """
    epsilon = 10e-12    
    return -log(p+epsilon) if y==1 else -log(1-p+epsilon)

def evaluateResults(lrModel, data):
    """Calculates the log loss for the data given the model.

    Args:
        model (LogisticRegressionModel): A trained logistic regression model.
        data (RDD of LabeledPoint): Labels and features for each observation.

    Returns:
        float: Log loss for the data.
    """    
    return data.map(lambda p: computeLogLoss(getP(p.features, lrModel.weights, lrModel.intercept), p.label)).mean()

# misc
def logTime(): return str(datetime.now())
def getFP(index): yield max(index)
def getTP(label): yield sum(label)

# calculate AUC score
def getAUCfromRdd(rddData, lrModel):
    labelsAndScores = rddData.map(lambda lp: (lp.label, getP(lp.features, lrModel.weights, lrModel.intercept)))
    if labelsAndScores.getNumPartitions() < 100:   
        labelAndIndex = labelsAndScores.repartition(100).sortBy(lambda (k,v): v, ascending=False).zipWithIndex()
    else:
        labelAndIndex = labelsAndScores.sortBy(lambda (k,v): v, ascending=False).zipWithIndex()
    labelAndIndex.cache()

    truePositives = np.cumsum(labelAndIndex.map(lambda l: l[0][0]).mapPartitions(getTP).collect())
    falsePositives = labelAndIndex.map(lambda l: l[1]+1).mapPartitions(getFP).collect() - truePositives
    numPositive = truePositives[-1]
    length = labelAndIndex.count()

    truePositiveRate = truePositives / numPositive
    falsePositiveRate = falsePositives / (length - numPositive)
    return metrics.auc(falsePositiveRate, truePositiveRate)

def encodeData(sc, numBuckets):
    rawTrainData = sc.textFile('s3://criteo-dataset/rawdata/train/part*', 180).map(lambda x: x.replace('\t', ','))
    rawValidationData = sc.textFile('s3://criteo-dataset/rawdata/validation/part*', 180).map(lambda x: x.replace('\t', ','))
    rawTestData = sc.textFile('s3://criteo-dataset/rawdata/test/part*', 180).map(lambda x: x.replace('\t', ','))

    # data encoding
    hashTrainData = rawTrainData.map(lambda p: parseHashPoint(p, numBuckets))
    hashTrainData.cache()
    hashValidationData = rawValidationData.map(lambda p: parseHashPoint(p, numBuckets))
    hashValidationData.cache()
    hashTestData = rawTestData.map(lambda p: parseHashPoint(p, numBuckets))
    hashTestData.cache()
    
    return hashTrainData, hashValidationData, hashTestData