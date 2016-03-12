#!/usr/bin/python



from PageRankIter import PageRankIter
from PageRankRedist import PageRankRedist
from PageRankSort2 import PageRankSort
from helper import getCounter
from subprocess import call, check_output
from time import time
import sys, getopt, datetime, os

# parse parameter
if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hg:j:i:")
    except getopt.GetoptError:
        print 'RunBFS.py -g <graph> -j <jump> -i <iteration> -m <mode> -w <weighted>'
        sys.exit(2)
    if len(opts) != 3:
        print 'RunBFS.py -g <graph> -j <jump> -i <iteration>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'RunBFS.py -g <graph> -j <jump> -i <iteration>'
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

# clearn direcotry
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/out'], stdout=FNULL)
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/rank'], stdout=FNULL)

# creat initialization job
print '%s: evaluating PageRank on \'%s\' for %d iterations ...' %(str(datetime.datetime.now()),
          graph[graph.rfind('/')+1:], n_iter)
init_job = PageRankIter(args=[graph, '--i', '1', '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])

# run initialization job
print str(datetime.datetime.now()) + ': running iteration 1 ...'
with init_job.make_runner() as runner:    
    runner.run()

# checking counter 
#print str(datetime.datetime.now()) + ': checking counter ...'
n_node = getCounter('wiki_node_count', 'nodes')
n_dangling = getCounter('wiki_dangling_mass', 'mass')/1e10
print '%s: initialization complete: %d nodes, %d are dangling!' %(str(datetime.datetime.now()), n_node, n_dangling)

# run redistribution job
call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])
dist_job = PageRankRedist(args=['hdfs:///user/leiyang/in/part*', '--s', str(n_node), '--j', str(jump), '--n', '0', 
                                '--m', str(n_dangling), '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])
print str(datetime.datetime.now()) + ': redistributing loss mass ...'
with dist_job.make_runner() as runner:    
    runner.run()

# move results for next iteration
#print str(datetime.datetime.now()) + ': moving results for next iteration ...'
call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])

# create iteration job
iter_job = PageRankIter(args=['hdfs:///user/leiyang/in/part*', '--i', '0', 
                              '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])

# run pageRank iteratively
i = 2
while(1):
    print str(datetime.datetime.now()) + ': running iteration %d ...' %i
    with iter_job.make_runner() as runner:        
        runner.run()
    
    # check counter 
    #print str(datetime.datetime.now()) + ': checking counter for mass loss ...'    
    mass_loss = getCounter('wiki_dangling_mass', 'mass')/1e10
    #print str(datetime.datetime.now()) + ': total mass loss: %.4f' %(mass_loss)
    
    # move results for next iteration
    #print str(datetime.datetime.now()) + ': moving results for mass redistribution ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'])
        
    # run redistribution job
    dist_job = PageRankRedist(args=['hdfs:///user/leiyang/in/part*', '--s', str(n_node), '--j', str(jump), '--n', '0', 
                                '--m', str(mass_loss), '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/out'])
    print str(datetime.datetime.now()) + ': redistributing loss mass %.4f ...' %mass_loss
    with dist_job.make_runner() as runner:    
        runner.run()
    
    if i == n_iter:
        break
    
    # if more iteration needed
    i += 1    
    #print str(datetime.datetime.now()) + ': moving results for next iteration ...'
    call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
    call(['hdfs', 'dfs', '-mv', '/user/leiyang/out', '/user/leiyang/in'], stdout=FNULL)

# run sort job
print str(datetime.datetime.now()) + ': sorting PageRank ...'
sort_job = PageRankSort(args=['hdfs:///user/leiyang/out/part*', '--s', str(n_node), '--n', '100',
                              '-r', 'hadoop', '--output-dir', 'hdfs:///user/leiyang/rank'])
with sort_job.make_runner() as runner:    
    runner.run()

# clear results
#print str(datetime.datetime.now()) + ': clearing files ...'
#call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/in'], stdout=FNULL)
#call(['hdfs', 'dfs', '-rm', '-r', '/user/leiyang/out'], stdout=FNULL)


print str(datetime.datetime.now()) + ": PageRank job completes in %.1f minutes!\n" %((time()-start)/60.0)
call(['hdfs', 'dfs', '-cat', '/user/leiyang/rank/part*'])