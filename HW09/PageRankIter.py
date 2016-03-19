from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRankIter(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankIter, self).configure_options()        
        self.add_passthrough_option(
            '--i', dest='init', default='0', type='int',
            help='i: run initialization iteration (default 0)')    

    def mapper_job_init(self, _, line):        
        # parse line
        nid, adj = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'adj = %s' %adj
        exec cmd
        # initialize node struct        
        node = {'a':adj.keys(), 'p':0}
        rankMass = 1.0/len(adj)
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
            rankMass = 1.0*node['p'] / n_adj
            # emit pageRank mass        
            for m in node['a']:
                yield m, rankMass
        else:
            # track dangling mass with counter
            self.increment_counter('wiki_dangling_mass', 'mass', int(node['p']*1e10))
        # reset pageRank and emit node
        node['p'] = 0
        yield nid, node
    
    def debug(self):
        de = 'bug'
                
    # write a separate combiner ensure the integrity of the graph topology
    # no additional node object will be generated
    def combiner(self, nid, value):             
        rankMass, node = 0.0, None        
        # loop through all arrivals
        for v in value:            
            if isinstance(v, float):
                rankMass += v                
            else:
                node = v            
        # emit accumulative mass for nid       
        if node:
            node['p'] += rankMass
            yield nid, node
        else:
            yield nid, rankMass
    
    # reducer for initialization pass --> need to handle dangling nodes
    def reducer_job_init(self, nid, value):      
        # increase counter for node count
        self.increment_counter('wiki_node_count', 'nodes', 1)
        rankMass, node = 0.0, None
        # loop through all arrivals
        for v in value:            
            if isinstance(v, float):
                rankMass += v         
            else:
                node = v
        # handle dangling node, create node struct and add missing mass
        if not node:            
            node = {'a':[], 'p':rankMass}            
            self.increment_counter('wiki_dangling_mass', 'mass', int(1e10))
        else:
            node['p'] += rankMass            
        # emit for next iteration
        yield nid, node
        
    # reducer for regular pass --> all nodes has structure available
    def reducer_job_iter(self, nid, value):              
        rankMass, node = 0.0, None
        # loop through all arrivals
        for v in value:            
            if isinstance(v, float):
                rankMass += v         
            else:
                node = v
        # update pageRank
        node['p'] += rankMass            
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
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankIter.run()