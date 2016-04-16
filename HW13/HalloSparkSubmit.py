from pyspark import SparkContext

sc = SparkContext()
y = sc.textFile('hdfs://localhost:9000/user/leiyang/PageRank-test.txt').cache()
print y.collect()
#y.saveAsTextFile('halloSaveFile')
print sc.appName
print sc.master
