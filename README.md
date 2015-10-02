# Small_Dense_Subgraphs
A Hadoop java implementation of a MapReduce algorithm for finding small dense subgraphs from any big interconnection network represented as a graph.
Let G = (V,E) be an undirected graph. For any subset of vertices V' from V, the density "rho" of the subgraph induced by V is defined as:
                            rho(V') = |E[V']|/V',
where |E[V']| subset of E is the set of edges whose endpoints lie in V'. In this problem we are asked to design and implement a MapReduce algorithm that, given a density rho, finds a subgraph of G with density >= rho, i.e., a set V' such that rho(V') >= rho. The challenge is to find a subgraph that is as small as possible. While the problem of finding the smallest dense-enough subgraph is hard, we should attempt at finding a subgraph with density >= rho as small as we can.
Several real-world graphs on which we can test our algorithms can be downloaded from the SNAP (Stanford Network Analysis Project) website: http://snap.stanford.edu/. Some of these graphs are directed, in which case one should take care of preprocessing them so that they are regarded as undirected in our algorithm.
