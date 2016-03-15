#!/usr/bin/python

from PageRankIter_T import PageRankIter_T
from PageRankDist_T import PageRankDist_T
from PageRankSort_2 import PageRankSort_T
from PageRankJoin import PageRankJoin
from helper import getCounter, getCounters
from subprocess import call, check_output
from time import time
import sys, getopt, datetime, os

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:j:i:d:s:")
    except getopt.GetoptError:
        print 'RunBFS.py -g <graph> -j <jump> -i <iteration> -d <index> -s <size>'
        sys.exit(2)
    if len(opts) != 5:
        print 'RunBFS.py -g <graph> -j <jump> -i <iteration> -d <index>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'RunBFS.py -g <graph> -j <jump> -i <iteration> -d <index>'
            sys.exit(2)
        elif opt == '-g':
            graph = arg
        elif opt == '-j':
            jump = arg
        elif opt == '-i':            
            n_iter = arg
        elif opt == '-d':
            index = arg
        elif opt == '-s':
            n_node = arg
        
start = time()
FNULL = open(os.devnull, 'w')
n_iter = int(n_iter)
doJoin = index!='NULL'
doInit = n_node=='0'
host = 'localhost'

print '%s: %s topic sensitive PageRanking on \'%s\' for %d iterations with damping factor %.2f ...' %(str(datetime.datetime.now()),
          'start' if doInit else 'continue', graph[graph.rfind('/')+1:], n_iter, 1-float(jump))

if doInit:
    # clear directory
    print str(datetime.datetime.now()) + ': clearing directory ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/out'], stdout=FNULL)
    
    # creat initialization job    
    init_job = PageRankIter_T(args=[graph, '--i', '1', '--n', '10', '-r', 'hadoop', 
                                    '--output-dir', 'hdfs:///user/leiyang/out'])

    # run initialization job
    print str(datetime.datetime.now()) + ': running iteration 1 ...'
    with init_job.make_runner() as runner:    
        runner.run()

    # checking counters
    n_node = getCounter('wiki_node_count', 'nodes', host)    
    loss = getCounters('wiki_dangling_mass', host)
    loss_array = ['0']*11
    for k in loss:
        i = int(k.split('_')[1])
        loss_array[i] = str(loss[k]/1e10)
    print '%s: initialization complete: %d nodes!' %(str(datetime.datetime.now()), n_node)

    #!python PageRankDist_T.py test.t --m '[1]*11' --s '100' --file './data/randNet_topics.txt'  -r 'hadoop' > test2.t
    # run redistribution job
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])
    loss_param = '[%s]' %(','.join(['0']*11) if len(loss)==0 else ','.join(loss_array))
    dist_job = PageRankDist_T(args=['hdfs:///user/leiyang/in/part*', '--s', str(n_node), '--m', loss_param,
                                    '--file', 'hdfs:///user/leiyang/randNet_topics.txt',
                                    '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])
    print str(datetime.datetime.now()) + ': distributing loss mass ...'
    with dist_job.make_runner() as runner:    
        runner.run()

# move results for next iteration
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])

# create iteration job
iter_job = PageRankIter_T(args=['hdfs:///user/leiyang/in/part*', '--i', '0', '--n', '10',
                              '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])

# run pageRank iteratively
i = 2 if doInit else 1
while(1):
    print str(datetime.datetime.now()) + ': running iteration %d ...' %i
    with iter_job.make_runner() as runner:        
        runner.run()
    
    # check counters for topic loss mass
    loss = getCounters('wiki_dangling_mass', host)
    loss_array = ['0']*11
    for k in loss:
        i = int(k.split('_')[1])
        loss_array[i] = str(loss[k]/1e10)
    
    # move results for next iteration
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])
        
    # run redistribution job
    loss_param = '[%s]' %(','.join(['0']*11) if len(loss)==0 else ','.join(loss_array))
    dist_job = PageRankDist_T(args=['hdfs:///user/leiyang/in/part*', '--s', str(n_node), '--m', loss_param,
                                    '--file', 'hdfs:///user/leiyang/randNet_topics.txt',
                                    '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])
    
    print str(datetime.datetime.now()) + ': distributing loss mass ...'
    with dist_job.make_runner() as runner:    
        runner.run()
    
    if i == n_iter:
        break
    
    # if more iteration needed
    i += 1    
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'], stdout=FNULL)

# run sort job
print str(datetime.datetime.now()) + ': sorting PageRank ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/rank'], stdout=FNULL)
sort_job = PageRankSort_T(args=['hdfs:///user/leiyang/out/part*', '--file', 'hdfs:///user/leiyang/randNet_topics.txt',
                              '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/rank'])

with sort_job.make_runner() as runner:    
    runner.run()
    
# run join job
if doJoin:
    print str(datetime.datetime.now()) + ': joining PageRank with index ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/join'], stdout=FNULL)
    join_job = PageRankJoin(args=[index, '-r', 'hadoop', '--file', 'hdfs:///user/leiyang/rank/part-00000', 
                                  '--output-dir', 'hdfs:///user/leiyang/join'])
    with join_job.make_runner() as runner:
        runner.run()

print "%s: PageRank job completes in %.1f minutes!\n" %(str(datetime.datetime.now()), (time()-start)/60.0)


call(['hdfs', 'dfs', '-cat', '/user/leiyang/join/p*' if doJoin else '/user/leiyang/rank/p*', '>'])