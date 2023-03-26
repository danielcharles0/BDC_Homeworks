import array
from pyspark import SparkContext, SparkConf
from collections import defaultdict
import sys
import os
import random


#Triangles counter in a group of edges
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



#ALGORITHM 1'S IMPLEMENTATION
#input:
#		edges - RDD with the edges
#		C - number of colors
#output:
#		t_final - estimate of the number of triangles
def MR_ApproxTCwithNodeColors(edges, C=1):
	p = 8191
	a = random.randint(1, p-1)
	b = random.randint(0, p-1)

	#input:
	#		e - edge
	#output:
	#		edge with color as key, color = -1 if non-monochromatic edge
	def hash(e):
		h1 = ((a * e[0] + b) % p) % C
		h2 = ((a * e[1] + b) % p) % C

		if (h1 == h2):
			return (h1, (e[0], e[1]))
		return (-1, (e[0], e[1]))

	#ROUND 1	
	rdd = edges.map(hash).filter(lambda x: x[0] != -1).groupByKey()		#MAP PHASE
	rdd = rdd.mapValues(lambda x: CountTriangles(x))		#REDUCE PHASE

	#ROUND 2
	#sum up all elements in triangles
	t = rdd.map(lambda x: x[1]).reduce(lambda x,y: x+y)
	#t = rdd.values().sum()

	t_final = C**2 * t

	return t_final



#ALGORITHM 2'S IMPLEMENTATION
#input:
#		edges - RDD with edges
#		C - number of partitions
#output:
#		t - estimate of the number of triangles
def MR_ApproxTCwithSparkPartitions(edges, C=1):
	#ROUND 1
	#C random partitions using mapPartitions
	rdd = edges.mapPartitions(partitioning, preservesPartitioning=False)

	#ROUND 2
	#sum up all elements in triangles
	t = C**2 #* sum(triangles)

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
	assert C.isdigit(), "C must be an integer - C is # of colours"
	C = int(C)

	# 2. Read number of runs (ONE RUN = Round1 + Round2)
	R = sys.argv[2]
	assert R.isdigit(), "R must be an integer - R = is # of runs"
	R = int(R)

	# 3. Read input file: in this case it'll be a .txt file
	data_path = sys.argv[3]
	assert os.path.isfile(data_path), "File or folder not found"
	rawData = sc.textFile(data_path).cache() 	#RDD of Strings
	edges = rawData.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).cache()		#RDD of integers
	
	print("\nEDGES:", edges.count())	

	runs_alg1 = [0] * R 	#results stored to compute median
	print("\nMR_ApproxTCwithNodeColors:")
	for i in range(R):
		runs_alg1[i] = MR_ApproxTCwithNodeColors(edges, C)
		print("\tRUN", i, "-> Estimate of t:", runs_alg1[i])	

	#runs_alg2 = [0] * R 	#results stored to compute median
	#print("\nMR_ApproxTCwithSparkPartitions:")
	#for i in range(R):
	#	runs_alg2[i] = MR_ApproxTCwithSparkPartitions(edges, C)
	#	print("\tRUN", i, "-> Estimate of t:", runs_alg2[i])	




if __name__ == "__main__":
	main()