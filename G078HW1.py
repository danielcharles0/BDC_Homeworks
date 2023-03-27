import array
from xml.dom.minidom import Identified
from pyspark import SparkContext, SparkConf
from collections import defaultdict
import sys
import os
import random
import time


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



#ALGORITHM 1
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
	#		(color,edge): color = -1 if non monochromatical edge
	def hash(e):
		h1 = ((a * e[0] + b) % p) % C
		h2 = ((a * e[1] + b) % p) % C

		if (h1 == h2):
			return (h1, (e[0], e[1]))
		return (-1, (e[0], e[1]))

	#ROUND 1	
	rdd = (edges.map(hash).filter(lambda x: x[0] != -1)		#MAP PHASE
		.groupByKey()										#SHUFFLE PHASE	
		.mapValues(lambda x: CountTriangles(x)))			#REDUCE PHASE

	#ROUND 2
	#Summing number of triangles for each color
	t = (rdd.map(lambda x: x[1])		#MAP PHASE
		.reduce(lambda x,y: x+y))		#REDUCE PHASE

	#estimated number of triangles formed by the input edges
	t_final = C**2 * t

	return t_final



#ALGORITHM 2'S IMPLEMENTATION
#input:
#		edges - RDD with edges
#		C - number of partitions
#output:
#		t_final - estimate of the number of triangles
def MR_ApproxTCwithSparkPartitions(edges, C=1):
	#ROUND 1
	#C random partitions using mapPartitions
	rdd = (edges.repartition(C)							#MAP
		.mapPartitions(lambda x: CountTriangles(x)))	#REDUCE	

	#ROUND 2
	#sum up all elements in triangles
	t = rdd.reduce(lambda x,y: x+y)
	
	#t = 0
	#for e in rdd.glom().collect():
	#		partial = 0
	#	for i in range(len(e)):
	#		partial = partial + int(e[-i-1]) * 10**i
	#	t = t + partial

	t_final = C**2 * t

	return t_final



def main():
	# CHECKING NUMBER OF CMD LINE PARAMETERS
	assert len(sys.argv) == 4, "Usage: python G078HW1.py <C> <R> <file_name>"

	# SPARK SETUP
	conf = SparkConf().setAppName('G078HW1').setMaster("local[*]")
	sc = SparkContext(conf=conf)

	# INPUT READING
	# 1. Read number of colors
	C = sys.argv[1]
	assert C.isdigit(), "C must be an integer - C is the number of colours"
	C = int(C)

	# 2. Read number of runs (ONE RUN = Round1 + Round2)
	R = sys.argv[2]
	assert R.isdigit(), "R must be an integer - R is the number of runs"
	R = int(R)

	# 3. Read input file: in this case it'll be a .txt file
	data_path = sys.argv[3]
	assert os.path.isfile(data_path), "File or folder not found"
	rawData = sc.textFile(data_path).cache() 	#RDD of Strings
	edges = rawData.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).cache()		#RDD of integers
	
	#printing # of edges
	print("\nEDGES:", edges.count())	

	#ALGORITHM 1 RUNS
	print("\nMR_ApproxTCwithNodeColors:")
	results_alg1 = [0] * R 		#results stored to compute median
	runningTime_alg1 = [0] * R 		#running time for run i
	for i in range(R):
		start = time.time() * 1000		#starting time in milliseconds
		results_alg1[i] = MR_ApproxTCwithNodeColors(edges, C)
		stop = time.time() * 1000		#stopping time in milliseconds
		runningTime_alg1[i] = stop - start
		print("\tRUN", i, "-> Estimate of t:", results_alg1[i])

	#Printing the median of R runs
	results_alg1.sort()
	if(R%2==1):
		median_alg1 = results_alg1[int(R/2)]
	else:
		median_alg1 = (results_alg1[R/2] + results_alg1[R/2-1])/2

	print("\n\tMEDIAN:", median_alg1)
	print("\n\tMEAN RUNNING TIME (ms):", sum(runningTime_alg1)/R)


	#ALGORITHM 2 RUNS
	print("\nMR_ApproxTCwithSparkPartitions:")
	results_alg2 = [0] * R 		#results stored to compute median
	runningTime_alg2 = [0] * R 		#running time for run i
	for i in range(R):
		start = time.time() * 1000		#starting time in milliseconds
		results_alg2[i] = MR_ApproxTCwithSparkPartitions(edges, C)
		stop = time.time() * 1000		#stopping time in milliseconds
		runningTime_alg2[i] = stop - start
		print("\tRUN", i, "-> Estimate of t:", results_alg2[i])

	#Printing the median of R runs
	results_alg2.sort()
	if(R%2==1):
		median_alg2 = results_alg2[int(R/2)]
	else:
		median_alg2 = (results_alg2[R/2] + results_alg2[R/2-1])/2

	print("\n\tMEDIAN:", median_alg2)
	print("\n\tMEAN RUNNING TIME (ms):", sum(runningTime_alg2)/R)




if __name__ == "__main__":
	main()