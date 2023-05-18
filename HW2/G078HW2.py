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



#Triangles counter based on key
def countTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:

        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
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

	#Estimated number of triangles formed by the input edges
	t_final = C**2 * t

	return t_final



#ALGORITHM 2
#input:
#		edges - RDD with the edges
#		C - number of colors
#output:
#		t_final - estimate of the number of triangles
def MR_ExactTC(edges, C):
	p = 8191
	a = random.randint(1, p-1)
	b = random.randint(0, p-1)

	#input:
	#		e - edge
	#output:
	#		(hash_min, hash_max): hash_min is the min hash between the two generated from the nodes
	def hash(e):
		h_u = ((a * e[0] + b) % p) % C
		h_v = ((a * e[1] + b) % p) % C

		if(h_u <= h_v):
			return [h_u, h_v]
		else:
			return [h_v, h_u]

	#input:
	#		e - edge
	#output:
	#		tuples - array containing tuples (k_i, e) with i=0,...,C-1
	def createTuples(e):
		arr = hash(e)
		tuples = [0] * C

		for i in range(C):
			if(i <= arr[0]):
				k = (i, arr[0], arr[1])
			elif(i <= arr[1]):
				k = (arr[0], i, arr[1])
			else:
				k = (arr[0], arr[1], i)

			tuples[i] = (k, e)

		return tuples

	#ROUND 1
	rdd = (edges.flatMap(createTuples)								#MAP 
		.groupByKey()												#SHUFFLE
		.map(lambda x: countTriangles2(x[0], x[1], a, b, p, C)))	#REDUCE

	#ROUND 2
	t_final = rdd.reduce(lambda x,y: x+y)

	return t_final



def main():
	# SPARK SETUP
	conf = SparkConf().setAppName('G078HW2').setMaster("local[*]").set("spark.locality.wait", "0s")
	sc = SparkContext(conf=conf)

	# INPUT READING
	# 1. Read number of colors
	C = sys.argv[1]
	C = int(C)

	# 2. Read number of runs
	R = sys.argv[2]
	R = int(R)

	# 3. Read flag
	F = sys.argv[3]
	F = int(F)

	# 4. Read input file: in this case it'll be a .txt file
	data_path = sys.argv[4]
	rawData = sc.textFile(data_path) 	#RDD of Strings
	edges = rawData.map(lambda x: (int(x.split(",")[0]), int(x.split(",")[1])))		#RDD of integers
	edges = edges.repartition(32).cache()

	#General info to be printed
	text = "Dataset = " + str(data_path) + "\n"
	text += "Number of Edges = " + str(edges.count()) + "\n"
	text += "Number of Colors = " + str(C) + "\n"
	text += "Number of Repetitions = " + str(R) + "\n"

	if(F==0):
		#ALGORITHM 1 RUNS
		text += "Approximation of algorithm with node coloring\n"
		results_alg1 = [0] * R 		#Results stored to compute median
		runningTime_alg1 = [0] * R 		#Running time for run i
		for i in range(R):
			start = time.time() * 1000		#Starting time in milliseconds
			results_alg1[i] = MR_ApproxTCwithNodeColors(edges, C)
			stop = time.time() * 1000		#Stopping time in milliseconds
			runningTime_alg1[i] = stop - start

		#Printing the median of R runs
		results_alg1.sort()
		if(R%2==1):
			median_alg1 = results_alg1[int(R/2)]
		else:
			median_alg1 = (results_alg1[int(R/2)] + results_alg1[int(R/2-1)])/2

		text += "- Number of triangles (median over " + str(R) + " runs = " + str(median_alg1) + "\n"
		text += "- Running time (average over " + str(R) + " runs) = " + str(sum(runningTime_alg1)/R) + " ms\n"
	
	elif(F==1):
		#ALGORITHM 2 RUN
		text += "Exact algorithm with node coloring\n"
		results_alg2 = [0] * R 		#Results stored to compute median
		runningTime_alg2 = [0] * R 		#Running time for run i
		for i in range(R):
			start = time.time() * 1000		#Starting time in milliseconds
			results_alg2[i] = MR_ExactTC(edges, C)
			stop = time.time() * 1000		#Stopping time in milliseconds
			runningTime_alg2[i] = stop - start
		
		text += "- Number of triangles = " + str(results_alg2[R-1]) + "\n"
		text += "- Running time (average over " + str(R) + " runs) = " + str(sum(runningTime_alg2)/R) + " ms\n"

	print("\n" + text)

if __name__ == "__main__":
	main()




