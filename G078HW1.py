from pyspark import SparkContext, SparkConf
from collections import defaultdict
import sys
import os
import random

#hash function (to be completed)
#input: 
#		edges - set of edges
#		C - number of subsets of edges
#output:
#		newEdges - new set of edges
def hash(edges, C=1):
	newEdges = set()
	p = 8191
	a = random.randint(1, p-1)
	b = random.randint(0, p-1)

	for e in edges:
		h1 = ((a * e[0] + b) % p) % C
		h2 = ((a * e[1] + b) % p) % C
		newEdges.add((h1, h2))

	return newEdges

def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

#
#input:
#		edges - RDD with the edges
#		C - number of colors
#output:
#		t - estimate of the number of triangles
def MR_ApproxTCwithNodeColors(edges, C):
	edges = hash(edges, C)

	#list of empty lists
	E = []
	for i in range(C):
		E.append([])

	#edge added if endpoints have same color
	for e in edges:
		if e(0) == e(1):
			E(e(0)).append(e)

	#TODO
	#problem: .flatmap requires an RDD, so we need to color edges inside the RDD
	#	not creating another data structure
	t = (edges.flatMap(CountTriangles) # <-- MAP PHASE (R1)
				 .reduceByKey(lambda x, y: x + y)) # <-- REDUCE PHASE (R1)
	return t

def main():
	# CHECKING NUMBER OF CMD LINE PARAMETERS
	assert len(sys.argv) == 4, "Usage: python G078HW1.py <C> <R> <file_name>"

	# SPARK SETUP
	conf = SparkConf().setAppName('G078HW1').setMaster("local[*]")
	sc = SparkContext(conf=conf)

	# INPUT READING

	# 1. Read number of colors
	C = sys.argv[1]
	assert C.isdigit(), "C must be an integer"
	C = int(C)

	# 2. Read number of rounds
	R = sys.argv[2]
	assert R.isdigit(), "R must be an integer"
	R = int(R)

	# 3. Read input file
	data_path = sys.argv[3]
	assert os.path.isfile(data_path), "File or folder not found"
	rawData = sc.textFile(data_path).cache() 	#RDD of Strings
	edges = rawData.map(lambda x: (int(x.split(",")(0)), int(x.split(",")(1)))).cache()		#RDD of integers

	#trying to understand RDD usage
	t = MR_ApproxTCwithNodeColors(edges, C)
	print("Estimate of t: ")		

if __name__ == "__main__":
	main()