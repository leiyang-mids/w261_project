from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRankDist(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankDist, self).configure_options()        
        self.add_passthrough_option(
            '--s', dest='size', default=0, type='int',
            help='size: node number (default 0)')    
        self.add_passthrough_option(
            '--j', dest='alpha', default=0.15, type='float',
            help='jump: random jump factor (default 0.15)') 
        self.add_passthrough_option(
            '--n', dest='norm', default=0, type='int',
            help='norm: normalize pageRank with graph size (default 0)') 
        self.add_passthrough_option(
            '--m', dest='m', default=0, type='float',
            help='m: rank mass from dangling nodes (default 0)') 
    
    def mapper_init(self):
        self.damping = 1 - self.options.alpha        
        self.p_dangling = self.options.m / self.options.size        
    
    # needed after initialization, after node number becomes available
    def mapper_norm(self, _, line):        
        # parse line
        nid, node = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %node
        exec cmd
        # get final pageRank      
        node['p'] = ((self.p_dangling + node['p'])*self.damping+self.options.alpha) / self.options.size
        yield nid, node
            
    def mapper(self, _, line):             
        # parse line
        nid, node = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %node
        exec cmd
        # get final pageRank      
        node['p'] = (self.p_dangling + node['p']) * self.damping + self.options.alpha
        yield nid, node

    def steps(self):
        jc = {
            'mapreduce.job.maps': '2',           
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper_norm if self.options.norm else self.mapper                       
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankDist.run()