from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRankSort(MRJob):
    #DEFAULT_PROTOCOL = 'json'   
    
    def configure_options(self):
        super(PageRankSort, self).configure_options()        
        self.add_passthrough_option(
            '--s', dest='size', default=0, type='int',
            help='size: node number (default 0)')
        self.add_passthrough_option(
            '--n', dest='top', default=100, type='int',
            help='size: node number (default 100)')
    
    def mapper(self, _, line):        
        # parse line
        nid, node = line.strip().split('\t', 1)
        cmd = 'node = %s' %node
        exec cmd        
        yield node['p'], nid.strip('"')
    
    def reducer_init(self):        
        self.i = 0
        self.total = 0
    
    def reducer(self, pageRank, nid): 
        for n in nid:
            if self.i < self.options.top:
                self.i += 1
                self.total += pageRank
                yield n, pageRank/self.options.size
            
    def reducer_final(self):
        yield 'total mass: ', self.total/self.options.size

    def steps(self):
        jc = {
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k1,1nr',
            'mapreduce.job.maps': '2', 
            'mapreduce.job.reduces': '1',  # must be 1 for sorting
        }
        return [MRStep(mapper=self.mapper, reducer_init=self.reducer_init, 
                       reducer=self.reducer, reducer_final=self.reducer_final
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankSort.run()