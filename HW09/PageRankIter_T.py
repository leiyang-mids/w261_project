from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRankIter_T(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankIter_T, self).configure_options()        
        self.add_passthrough_option(
            '--i', dest='init', default='0', type='int',
            help='i: run initialization iteration (default 0)') 
        self.add_passthrough_option(
            '--n', dest='n_topic', default='1', type='int',
            help='n: number of topics (default 1)') 

    def mapper_job_init(self, _, line):        
        # parse line
        nid, adj = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'adj = %s' %adj
        exec cmd
        # initialize node struct        
        node = {'a':adj.keys(), 'p':[0]*(self.options.n_topic + 1)}
        # vanillar PageRank and topic sensitive PageRank
        rankMass = [1.0 / len(adj)] * (self.options.n_topic + 1)
        # emit node
        yield nid, node
        # emit pageRank mass        
        for m in node['a']:
            yield m, rankMass
            
    def mapper_job_iter(self, _, line):             
        # parse line
        nid, node = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %node
        exec cmd
        # distribute rank mass  
        n_adj = len(node['a'])
        if n_adj > 0:
            rankMass = [x / n_adj for x in node['p']]
            # emit pageRank mass        
            for m in node['a']:
                yield m, rankMass
        else:
            # track dangling mass with counters
            for i in range(self.options.n_topic+1):
                self.increment_counter('wiki_dangling_mass', 'mass_%d' %i, int(node['p'][i]*1e10))
        # reset pageRank and emit node
        node['p'] = [0]*(self.options.n_topic+1)
        yield nid, node
    
    def debug(self):
        de = 'bug'
                
    # write a separate combiner ensure the integrity of the graph topology
    # no additional node object will be generated
    def combiner(self, nid, value):             
        rankMass, node = [0]*(self.options.n_topic+1), None        
        # loop through all arrivals
        for v in value:            
            if isinstance(v, list):
                rankMass = [a+b for a,b in zip(rankMass, v)]                
            else:
                node = v            
        # emit accumulative mass for nid       
        if node:
            node['p'] = [a+b for a,b in zip(rankMass, node['p'])]
            yield nid, node
        else:
            yield nid, rankMass
    
    # reducer for initialization pass --> need to handle dangling nodes
    def reducer_job_init(self, nid, value):      
        # increase counter for node count
        self.increment_counter('wiki_node_count', 'nodes', 1)
        rankMass, node = [0]*(self.options.n_topic+1), None
        # loop through all arrivals
        for v in value:            
            if isinstance(v, list):
                rankMass = [a+b for a,b in zip(rankMass, v)]         
            else:
                node = v
        # handle dangling node, create node struct and add missing mass
        if not node:            
            node = {'a':[], 'p':rankMass}   
            for i in range(self.options.n_topic+1):
                self.increment_counter('wiki_dangling_mass', 'mass_%d' %i, int(1e10))
        else:
            node['p'] = [a+b for a,b in zip(rankMass, node['p'])]
        # emit for next iteration
        yield nid, node
        
    # reducer for regular pass --> all nodes has structure available
    def reducer_job_iter(self, nid, value):              
        rankMass, node = [0]*(self.options.n_topic+1), None
        # loop through all arrivals
        for v in value:            
            if isinstance(v, list):
                rankMass = [a+b for a,b in zip(rankMass, v)]        
            else:
                node = v
        # update pageRank
        node['p'] = [a+b for a,b in zip(rankMass, node['p'])]            
        # emit for next iteration
        yield nid, node

    def steps(self):
        jc = {
            'mapreduce.job.maps': '2',
            'mapreduce.job.reduces': '2',
        }
        return [MRStep(mapper=self.mapper_job_init if self.options.init else self.mapper_job_iter                       
                       , combiner=self.combiner                       
                       , reducer=self.reducer_job_init if self.options.init else self.reducer_job_iter
                       #, jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankIter_T.run()