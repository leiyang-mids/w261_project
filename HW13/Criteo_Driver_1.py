
from time import time, gmtime, strftime

execfile('CriteoHelper2.py')

# define parameters
print '%s: start logistic regression job ...' %(logTime())
numBucketsCTR = 5000
lrStep = 10
start = time()
sc = SparkContext()

# data preparaion
print '%s: preparing data ...' %(logTime())
dTrain, dVlidation, dTest = encodeData(numBucketsCTR)

# build model
print '%s: building logistic regression model ...' %(logTime())
model = LogisticRegressionWithSGD.train(dTrain, iterations=500, step=lrStep, regType=None, intercept=True)

# get log loss
print '%s: evaluating log loss ...' %(logTime())
logLossVa = evaluateResults(model, dValidation)
logLossTest = evaluateResults(model, dTest)
logLossTrain = evaluateResults(model, dTrain)

# get AUC
print '%s: evaluating AUC ...' %(logTime())
aucTrain = getAUCfromRdd(dTrain, model)
aucVal = getAUCfromRdd(dValidation, model)
aucTest = getAUCfromRdd(dTest, model)
print '\n%s: job completes in %.2f minutes!' %(logTime(), (time()-start)/60.0)

# show results
print '\n\t\t log loss \t\t\t AUC'
print 'Training:\t %.4f\t\t %.4f' %(logLossTrain, aucTrain)
print 'Validation:\t %.4f\t\t %.4f' %(logLossVa, aucVal)
print 'Test:\t %.4f\t %.4f' %(logLossTest, aucTest)