from mrjob.job import MRJob
from mrjob.step import MRStep
from subprocess import Popen, PIPE

class PageRankJoin(MRJob):
    #DEFAULT_PROTOCOL = 'json'   
    
    def mapper_init(self):
        self.topRanks = {}
        # read rand list, prepare for mapper in-memory join        
        cat = Popen(['cat', 'part-00000'], stdout=PIPE)
        for line in cat.stdout:
            nid, rank = line.strip().split('\t')
            self.topRanks[nid.strip('"')] = rank
    
    def mapper(self, _, line):        
        # parse line
        name, nid, d_in, d_out = line.strip().split('\t')
        if nid in self.topRanks:                        
            yield float(self.topRanks[nid]), '%s - %s' %(nid, name)
    
    def reducer(self, key, value):
        for v in value:
            yield key, v
    

    def steps(self):
        jc = {            
            'mapreduce.job.output.key.comparator.class': 'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
            'mapreduce.partition.keycomparator.options': '-k1,1nr',
            'mapreduce.job.maps': '2', 
            'mapreduce.job.reduces': '1',  # must be 1 for sorting            
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper
                       , reducer=self.reducer
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankJoin.run()
