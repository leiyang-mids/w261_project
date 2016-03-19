from mrjob.job import MRJob
from mrjob.step import MRStep
from subprocess import Popen, PIPE

class PageRankDist_W(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankDist_W, self).configure_options()
        self.add_passthrough_option(
            '--j', dest='alpha', default=0.15, type='float',
            help='jump: teleport factor (default 0.15)')
        self.add_passthrough_option(
            '--b', dest='beta', default=0.99, type='float',
            help='beta: topic bias factor (default 0.99)')
        self.add_passthrough_option(
            '--m', dest='m', default='', type='str',
            help='m: rank mass from dangling nodes')

    def mapper_init(self):
        # load topic file and count
        T_j = [1455304, 1536145, 1591290, 1624124, 1610195, 1550659, 1511681, 1472178, 1419076, 1421625]
        N = sum(T_j)
        # prepare adjustment factors
        self.damping = 1 - self.options.alpha
        cmd = 'm = %s' %self.options.m
        exec cmd
        # assuming here -m is specified with a list syntax string
        self.p_dangling = [1.0*x / N for x in m]
        # for each topic, get topic bias
        b = self.options.beta
        self.v_ij = [[1, 1]] + [[(1-b)*N/(N-t), b*N/t] for t in T_j]

    def mapper(self, _, line):
        # parse line
        nid, node = line.strip().split('\t', 1)
        nid = nid.strip('"')
        cmd = 'node = %s' %node
        exec cmd
        # get final pageRank
        for i in range(len(self.v_ij)):
            vij = self.v_ij[i][i==node['t']]
            node['p'][i] = (self.p_dangling[i]+node['p'][i])*self.damping + self.options.alpha*vij
        yield nid, node

    def steps(self):
        jc = {
            'mapreduce.job.maps': '25',           
        }
        return [MRStep(mapper_init=self.mapper_init
                       , mapper=self.mapper
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankDist_W.run()
