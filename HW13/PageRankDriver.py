from operator import add
from pyspark import SparkContext
from subprocess import call

execfile('PageRank.py')

# load original graph file
sc = SparkContext()

#graph_file = sc.textFile('hdfs:///user/leiyang/PageRank-test.txt')
#index_file = sc.textFile('hdfs:///user/leiyang/toy_index.txt')

graph_file = sc.textFile('hdfs:///user/leiyang/all-pages-indexed-out.txt', 80)
index_file = sc.textFile('hdfs:///user/leiyang/indices.txt', 16)

# initialize variables
nDangling = sc.accumulator(0)
lossMass = sc.accumulator(0.0)
damping = 0.85
alpha = 1 - damping
nTop, nIter = 200, 10
start = time()
print '%s: start PageRank initialization ...' %(logTime())
graph = graph_file.flatMap(initialize).reduceByKey(accumulateMass).map(getDangling) #.cache()
# get graph size
G = graph.count()
# broadcast dangling mass for redistribution
p_dangling = sc.broadcast(1.0*nDangling.value/G)
graph = graph.map(redistributeMass)

print '%s: initialization completed, dangling node(s): %d, total nodes: %d' %(logTime(), nDangling.value, G)
# run page rank
for i in range(nIter-1):
    print '%s: running iteration %d ...' %(logTime(), i+2)
    lossMass.value = 0.0
    graph = graph.flatMap(distributeMass).reduceByKey(accumulateMass) #.cache() #checkpoint()?
    # need to call an action here in order to have loss mass
    graph.count()
    print '%s: redistributing loss mass: %.4f' %(logTime(), lossMass.value)
    p_dangling = sc.broadcast(lossMass.value/G)
    graph = graph.map(redistributeMass)

totalMass = graph.aggregate(0, (lambda x, y: y[1]['p'] + x), (lambda x, y: x+y))
print '%s: normalized weight of the graph: %.4f' %(logTime(), totalMass/G)
print '%s: PageRanking completed in %.2f minutes.' %(logTime(), (time()-start)/60.0)
# get the page name by join
topPages = graph.map(lambda n:(n[0],n[1]['p']/G)).sortBy(lambda n: n[1], ascending=False).take(nTop)
rankList = index_file.map(getIndex).join(sc.parallelize(topPages)).map(lambda l: l[1])
# save final rank list
call(['hdfs', 'dfs', '-rm', '-r', '/home/hadoop/lei/pageRank'])
rankList.sortBy(lambda n: n[1], ascending=False).saveAsTextFile('/home/hadoop/lei/pageRank')
print '%s: results saved, job completed!' %logTime()
print rankList.collect()
