from pyspark import SparkContext, SparkConf
from collections import defaultdict
import sys
import os
import random



#Triangles counter in a group of edges
def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges.collect():
        y, (u, v) = edge
        #print("YUV:", y, u, v)
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
def MR_ApproxTCwithNodeColors(edges, C=1):
	p = 8191
	a = random.randint(1, p-1)
	b = random.randint(0, p-1)

	#input:
	#		e - edge
	#output:
	#		-1 if edge non-monochromatic
	#		h1 otherwise
	def hash(e):
		h1 = ((a * e[0] + b) % p) % C
		h2 = ((a * e[1] + b) % p) % C

		if (h1 == h2):
			return h1
		return -1

	#function to be used in map step
	#input:
	#		e - edge as (u,v)
	#		i - current color analyzed
	#output:
	#		e - modified edge as (color, (u,v))
	def f(e, i):
		y = hash(e) 
		#print(e, "-> HASH:", y)
		
		if y == i:
			e = (y, (e[0], e[1]))
		else:
			e = (-1, (e[0], e[1]))
		return e

	#ROUND 1
	triangles = [0] * C		#list containing the number of triangles of partition i in position i
	for i in range(C):
		rdd = edges.map(lambda x: f(x, i)).filter(lambda x: x[0] == i)
		
		print("\tCOLOR", i, ":", rdd.count(), "ELEMENT(S)")
		for elem in rdd.collect():
			print("\t\t", elem)
		
		triangles[i] = CountTriangles(rdd)

	#ROUND 2
	#sum up all elements in triangles
	t = C**2 * sum(triangles)

	return t

#
#input:
#		edges - RDD with edges
#		C - number of partitions
#output:
#		t - estimate of the number of triangles
def MR_ApproxTCwithSparkPartitions(edges, C=1):
	#TODO
	#1. Partitioning
	edges = edges.repartition(C)

	#2. C random partitions using mapPartitions
	edges = (edges.mapPartitions(lambda x: x, preservesPartitioning=false)
			.reduceByKey())


	#sum up all elements in triangles
	t = 0
	for i in range(len(triangles)):
		t = t + triangles[i]
	t = t*C^2

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
	edges = rawData.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1]))).cache()		#RDD of integers

	print("\nEDGES:", edges.count())	

	#trying to understand RDD usage
	print("\nMR_ApproxTCwithNodeColors:")
	for i in range(R):
		t = MR_ApproxTCwithNodeColors(edges, C)
		print("\tRUN", i, "-> Estimate of t:", t)		

if __name__ == "__main__":
	main()