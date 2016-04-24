
from time import time
from pyspark import SparkContext

execfile('CriteoHelper.py')

# define parameters
print '%s: start logistic regression job ...' %(logTime())
numBucketsCTR = 1000
lrStep = 10
start = time()
sc = SparkContext()

# data preparaion
print '%s: preparing data ...' %(logTime())
#rawTrainData = sc.textFile('hdfs:///user/leiyang/criteo/rawTrain', 2).map(lambda x: x.replace('\t', ','))
#rawValidationData = sc.textFile('hdfs:///user/leiyang/criteo/rawValidation', 2).map(lambda x: x.replace('\t', ','))
#rawTestData = sc.textFile('hdfs:///user/leiyang/criteo/rawTest', 2).map(lambda x: x.replace('\t', ','))
rawTrainData = sc.textFile('s3://criteo-dataset/rawdata/train/part*', 80).map(lambda x: x.replace('\t', ','))
rawValidationData = sc.textFile('s3://criteo-dataset/rawdata/validation/part*', 80).map(lambda x: x.replace('\t', ','))
rawTestData = sc.textFile('s3://criteo-dataset/rawdata/test/part*', 80).map(lambda x: x.replace('\t', ','))


# data encoding
hashTrainData = rawTrainData.map(lambda p: parseHashPoint(p, numBucketsCTR))
hashTrainData.cache()
hashValidationData = rawValidationData.map(lambda p: parseHashPoint(p, numBucketsCTR))
hashValidationData.cache()
hashTestData = rawTestData.map(lambda p: parseHashPoint(p, numBucketsCTR))
hashTestData.cache()

# build model
print '%s: building logistic regression model ...' %(logTime())
model = LogisticRegressionWithSGD.train(hashTrainData, iterations=500, step=lrStep, regType=None, intercept=True)

# get log loss
print '%s: evaluating log loss ...' %(logTime())
logLossVa = evaluateResults(model, hashValidationData)
logLossTest = evaluateResults(model, hashTestData)
logLossTrain = evaluateResults(model, hashTrainData)

# get AUC
print '%s: evaluating AUC ...' %(logTime())
aucVal = getAUC(hashValidationData, model)
aucTrain = getAUC(hashTrainData, model)
aucTest = getAUC(hashTestData, model)
print '\n%s: job completes in %.2f minutes!' %(logTime(), (time()-start)/60.0)

# show results
print '\n\t\t log loss \t\t\t AUC'
print 'Training:\t %.4f\t\t %.4f' %(logLossTrain, aucTrain)
print 'Validation:\t %.4f\t\t %.4f' %(logLossVa, aucVal)
print 'Test:\t %.4f\t %.4f' %(logLossTest, aucTest)
