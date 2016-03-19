from mrjob.job import MRJob
from mrjob.step import MRStep

class PageRankInit(MRJob):
    DEFAULT_PROTOCOL = 'json'

    def configure_options(self):
        super(PageRankInit, self).configure_options()
        self.add_passthrough_option(
            '--n', dest='n_topic', default='0', type='int',
            help='n: number of topics (default 0)')

    def mapper(self, _, line):
        # parse line
        elem = line.strip().split('\t')
        if len(elem) == 2:
            nid, adj = elem[0].strip('"'), elem[1]
            cmd = 'adj = %s' %adj
            exec cmd
            # initialize node struct
            node = {'a':adj.keys(), 'p':[1.0]*(self.options.n_topic+1)}
            # emit node
            yield nid, node
            # emit pageRank mass
            for m in node['a']:
                yield m, {'f':0}
        else:
            yield elem[1].strip('"'), {'t':len(elem[0])%10 + 1}

    # write a separate combiner ensure the integrity of the graph topology
    # no additional node object will be generated
    def combiner(self, nid, value):
        node, topic, adj = None, None, None
        # loop through all arrivals
        for v in value:
            if 'a' in v:
                node = v
            elif 't' in v:
                topic = v
            elif 'f' in v:
                adj = v
        # emit accumulative mass for nid
        if node:
            yield nid, node
        if topic:
            yield nid, topic
        if adj:
            yield nid, adj

    # reducer for initialization pass --> need to handle dangling nodes
    def reducer(self, nid, value):
        topic, node = None, None
        # loop through all arrivals
        for v in value:
            if 't' in v:
                topic = v['t']
            elif 'a' in v:
                node = v
        # handle dangling node, create node struct and add missing mass
        if not node: # and topic:
            node = {'a':[], 'p':[1.0]*(self.options.n_topic+1)}
        node['t'] = topic if topic else 0
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
    PageRankInit.run()
