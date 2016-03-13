from mrjob.job import MRJob
from mrjob.step import MRStep
from subprocess import Popen, PIPE

class PageRankDist_T(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankDist_T, self).configure_options()        
        self.add_passthrough_option(
            '--s', dest='size', default=0, type='int',
            help='size: node number (default 0)')    
        self.add_passthrough_option(
            '--j', dest='alpha', default=0.15, type='float',
            help='jump: teleport factor (default 0.15)') 
        self.add_passthrough_option(
            '--b', dest='beta', default=0.99, type='float',
            help='beta: topic bias factor (default 0.99)') 
        self.add_passthrough_option(
            '--m', dest='m', default='', type='str',
            help='m: rank mass from dangling nodes') 
        self.add_passthrough_option(
            '--w', dest='wiki', default=0, type='int',
            help='w: if it is wiki data (default 1)') 
    
    def mapper_init(self):
        # load topic file and count
        T_j, self.T_index = {}, {}
        cat = Popen(['cat', 'randNet_topics.txt'], stdout=PIPE)
        for line in cat.stdout:
            nid, topic = line.strip().split('\t')
            self.T_index[nid] = topic
            if topic not in T_j:
                T_j[topic] = 1
            else:
                T_j[topic] += 1
        # prepare adjustment factors
        self.damping = 1 - self.options.alpha        
        cmd = 'm = %s' %self.options.m
        exec cmd
        self.p_dangling = [1.0*x / self.options.size for x in m]
        # for each topic, get topic bias
        self.v_ij = [[1, 1]]*(len(T_j)+1)
        N = self.options.size
        for t in T_j:
            self.v_ij[int(t)] = [(1-self.options.beta)*N/(N-T_j[t]), self.options.beta*N/T_j[t]]
            
        #yield T_j, (sum(T_j.values()), len(T_j))
        #yield self.T_index, len(self.T_index)
        #yield self.p_dangling, None
        #yield self.v_ij, None
        
            
    def mapper(self, _, line):             
        # parse line
        nid, node = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %node
        exec cmd
        # get final pageRank
        for i in range(len(self.v_ij)):
            isTopic = i==int(self.T_index[nid])
            node['p'][i] = (self.p_dangling[i]+node['p'][i])*self.damping+self.options.alpha*self.v_ij[i][isTopic]
        yield nid, node

    def steps(self):
        jc = {
            'mapreduce.job.maps': '2',           
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper                    
                       #, jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankDist_T.run()