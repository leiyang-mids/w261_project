from time import time
from datetime import datetime

def initialize(line):
    # parse line
    nid, adj = line.strip().split('\t', 1)
    exec 'adj = %s' %adj
    # initialize node struct
    node = {'a':adj.keys(), 'p':0}
    rankMass = 1.0/len(adj)
    # emit pageRank mass and node
    return [(m, rankMass) for m in node['a']] + [(nid.strip('"'), node)]

def accumulateMass(a, b):
    if isinstance(a, float) and isinstance(b, float):
        return a+b
    if isinstance(a, float) and not isinstance(b, float):
        b['p'] += a
        return b
    else:
        a['p'] += b
        return a

def getDangling(node):
    global nDangling
    if isinstance(node[1], float):
        nDangling += 1
        return (node[0], {'a':[], 'p':node[1]})
    else:
        return node

def redistributeMass(node):
    node[1]['p'] = (p_dangling.value+node[1]['p'])*damping + alpha
    return node

def distributeMass(node):
    global lossMass
    mass, adj = node[1]['p'], node[1]['a']
    node[1]['p'] = 0
    if len(adj) == 0:
        lossMass += mass
        return [node]
    else:
        rankMass = mass/len(adj)
        return [(x, rankMass) for x in adj]+[node]

def getIndex(line):
    elem = line.strip().split('\t')
    return (elem[1], elem[0])

def logTime():
    return str(datetime.now())
