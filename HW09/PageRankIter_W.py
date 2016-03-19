from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRankIter_W(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankIter_W, self).configure_options()
        self.add_passthrough_option(
            '--n', dest='n_topic', default='0', type='int',
            help='n: number of topics (default 0)')

    def mapper(self, _, line):
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
                yield m, {'m':rankMass}
        else:
            # track dangling mass for each topic with counters
            for i in range(self.options.n_topic+1):
                self.increment_counter('wiki_dangling_mass', 'topic_%d' %i, int(node['p'][i]*1e10))
        # reset pageRank and emit node
        node['p'] = [0]*(self.options.n_topic+1)
        yield nid, node


    # write a separate combiner ensure the integrity of the graph topology
    # no additional node object will be generated
    def combiner(self, nid, value):
        rankMass, node = [0]*(self.options.n_topic+1), None
        # loop through all arrivals
        for v in value:
            if 'm' in v:
                rankMass = [a+b for a,b in zip(rankMass, v['m'])]
            else:
                node = v
        # emit accumulative mass for nid
        if node:
            node['p'] = [a+b for a,b in zip(rankMass, node['p'])]
            yield nid, node
        else:
            yield nid, {'m':rankMass}

    def reducer(self, nid, value):
        rankMass, node = [0]*(self.options.n_topic+1), None
        # loop through all arrivals
        for v in value:
            if 'm' in v:
                rankMass = [a+b for a,b in zip(rankMass, v['m'])]
            else:
                node = v
        # update pageRank
        if True: #node:
            node['p'] = [a+b for a,b in zip(rankMass, node['p'])]
            # emit for next iteration
            yield nid, node

    def steps(self):
        jc = {
            'mapreduce.job.maps': '25',
            'mapreduce.job.reduces': '25',
        }
        return [MRStep(mapper=self.mapper
                       , combiner=self.combiner
                       , reducer=self.reducer
                       , jobconf = jc
                      )
               ]

if __name__ == '__main__':
    PageRankIter_W.run()
