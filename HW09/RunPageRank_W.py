#!/usr/bin/python

from PageRankIter_W import PageRankIter_W
from PageRankDist_W import PageRankDist_W
from PageRankSort_W import PageRankSort_W
from helper import getCounter, getCounters
from subprocess import call, check_output
from time import time
import sys, getopt, datetime, os

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:j:i:")
    except getopt.GetoptError:
        print 'RunBFS.py -g <graph> -j <jump> -i <iteration> -d <index> -s <size>'
        sys.exit(2)
    if len(opts) != 3:
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


start = time()
FNULL = open(os.devnull, 'w')
n_iter = int(n_iter)
host = 'localhost'

print '%s: %s topic sensitive PageRanking on \'%s\' for %d iterations with damping factor %.2f ...' %(str(datetime.datetime.now()),
          'start', graph[graph.rfind('/')+1:], n_iter, 1-float(jump))

# clear directory
print str(datetime.datetime.now()) + ': clearing directory ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/out'], stdout=FNULL)
call(['hdfs', 'dfs', '-cp', '/user/leiyang/wiki_topic', '/user/leiyang/in'])

# create iteration job
iter_job = PageRankIter_W(args=['hdfs:///user/leiyang/in/part*', '--n', '10',
                              '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])

# run pageRank iteratively
iteration = 1
while(1):
    print str(datetime.datetime.now()) + ': running iteration %d ...' %iteration
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
    dist_job = PageRankDist_W(args=['hdfs:///user/leiyang/in/part*', '--m', loss_param,
                                    '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])

    print str(datetime.datetime.now()) + ': distributing loss mass ...'
    with dist_job.make_runner() as runner:
        runner.run()

    if iteration == n_iter:
        break

    # if more iteration needed
    iteration += 1
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'], stdout=FNULL)

# run sort job
print str(datetime.datetime.now()) + ': sorting PageRank ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/rank'], stdout=FNULL)
sort_job = PageRankSort_W(args=['hdfs:///user/leiyang/out/part*',
                              '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/rank'])

with sort_job.make_runner() as runner:
    runner.run()

print "%s: PageRank job completes in %.1f minutes!\n" %(str(datetime.datetime.now()), (time()-start)/60.0)
call(['hdfs', 'dfs', '-cat', '/user/leiyang/rank/p*'])
