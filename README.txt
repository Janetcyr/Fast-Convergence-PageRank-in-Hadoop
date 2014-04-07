    	
Introduction

This is the final group programming project in Large Scale Information Systems. It uses AWS Elastic MapReduce to perform a much larger-scale computation for PageRank.

For this project we computed PageRank for a reasonably large Web graph (685230 nodes, 7600595 edges). Our answers was reasonably accurate: we achieved an L1 relative residual error below 0.1%. More on this below.

It is relatively straightforward to code PageRank in MapReduce using a direct node-by-node approach, equivalent to the “Power Iteration” technique. But the cost of this approach is rather high. In our experiments, this direct approach took more than 20 MapReduce passes to converge to the desired accuracy on our test graph.

To improve the convergence rate, we had mimic another technique discussed in class, Blocked Matrix Multiplication, in which we reduced the number of MapReduce passes by doing nontrivial computation in the reduce steps. We have already used the METIS graph partitioning software to partition our graph into Blocks of about 10,000 nodes each. A reduce task in Blocked PageRank computation, instead of updating the PageRank value of a single node, will do a full PageRank computation on an entire Block of the graph, using the current approximate PageRank values of the block’s neighbors as fixed boundary conditions. Using this technique, we would be able to achieve the required accuracy using no more than six or seven MapReduce passes.

AWS Elastic MapReduce was be used to manage the several MapReduce passes needed to identify the graph edges and blocks and compute the PageRank values.

This Project was done in groups of 3 persons. 