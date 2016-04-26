
from time import time, gmtime, strftime
from subprocess import call
from pyspark import SparkContext

execfile('CriteoHelper2.py')

# define parameter space
print '%s: start logistic regression job ...' %(logTime())
numBucketsCTR = [20000] #[10000] #[1000, 5000, 10000, 20000, 40000]
lrStep = [20, 10, 5, 1] #[5, 1]
regParams = [1e-7, 1e-5, 1e-3, 1e-1]
nSteps = len(numBucketsCTR)*len(lrStep)*len(regParams)
print '%s: bucket sizes: %s' %(logTime(), str(numBucketsCTR))
print '%s: LR steps: %s' %(logTime(), str(lrStep))
print '%s: regularization: %s' %(logTime(), str(regParams))
print '%s: total steps: %d' %(logTime(), nSteps)

# initialize 
start = time()
bestModel, bestLogLoss, bestAUC = None, 1e10, 0
sc = SparkContext()

iStep = 1
# grid search
for nBucket in numBucketsCTR:
    # data preparaion    
    dTrain, dValidation, dTest = encodeData(sc, nBucket)
    for stp in lrStep:
        for reg in regParams:
            # build model
            print '%s: step %d/%d starts with bucket-%d, step-%d, reg-%.9f, modeling ...' %(logTime(), iStep, nSteps,
                                                                                            nBucket, stp, reg)
            model = LogisticRegressionWithSGD.train(dTrain, iterations=500, step=stp, 
                                                    regParam=reg, regType='l2', intercept=True)
            # get log loss
            print '%s: evaluating log loss ...' %(logTime())
            logLossVa = evaluateResults(model, dValidation)
            # get AUC
            print '%s: evaluating AUC ...' %(logTime())            
            aucVal = getAUCfromRdd(dValidation, model)
            # compare model
            print '%s: step %d/%d completed, logLoss-%.4f, AUC-%.4f' %(logTime(), iStep, nSteps, logLossVa, aucVal)            
            if logLossVa < bestLogLoss:
                bestLogLoss, bestModel, bestAUC = logLossVa, model, aucVal                
            # save all results to s3, in case job crashes - aws s3 cp toy_index.txt s3://w261.data/HW13/toy.txt
            logName = 's3://w261.data/HW13/criteo_search_log_' + strftime("%d%b%Y_%H%M%S", gmtime())
            call(['aws', 's3', 'cp', '/home/hadoop/lei/criteo_search_log.txt', logName, '--region', 'us-west-2'])
            iStep += 1

# use best model to evaluate 
print '%s: grid search completed in %.2f minutes!' %(logTime(), (time()-start)/60.0)
print '%s: our best model has log loss %.4f and AUC %.4f' %(logTime(), bestLogLoss, bestAUC)

# show results
print '%s: checking log loss for test data ...' %logTime()
logLoss = evaluateResults(bestModel, dTest)
print '%s: checking AUC for test data ...' %logTime()
aucTest = getAUCfromRdd(dTest, bestModel)
print '%s: our best model has log loss %.4f and AUC %.4f on test data.' %(logTime(), logLoss, aucTest)

# save log to s3
print '\n%s: job completes in %.2f minutes!' %(logTime(), (time()-start)/60.0)
logName = 's3://w261.data/HW13/criteo_search_log_' + strftime("%d%b%Y_%H%M%S", gmtime())
call(['aws', 's3', 'cp', '/home/hadoop/lei/criteo_search_log', logName, '--region', 'us-west-2'])