from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import datetime

# After how many items should we stop?
THRESHOLD = 10000000

#TODO
#Count Sketch hash function h: U -> {1,2,...,W}
def hash1(key, row):
    h = (key * row) % W
    return h

#TODO
#Count Sketch hash function g: U -> {-1, +1}
def hash2(key, row):
    g = (key * row) % W
    if g >= W/2:
        g = 1
    else:
        g = -1
    return g



# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    start = initialTime.second + 100 * initialTime.minute
    current = time.second + 100*time.minute
    if (current - start) >= left and (current - start) <= right:
    
        # We are working on the batch at time `time`.
        global streamLength, histogram, C
        batch_size = batch.count()
        streamLength[0] += batch_size
        
        if batch_size != 0:
            batch_items = (batch.map(lambda s: (int(s), 1))
                .groupByKey()
                .map(lambda x: (x[0], sum(x[1]))))

            for t in batch_items.collect():
                key = t[0]

                #Exact computation
                if t[0] not in histogram:
                    histogram[key] = t[1]
                else:
                    histogram[key] += t[1]

                #Count Sketch
                for i in range(D):
                    C[i][hash1(key, i)] += hash2(key, i)



            #print("Current frequency of 9999 =", histogram[9999])
                    
            # If we wanted, here we could run some additional code on the global histogram
            if batch_size > 0:
                print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD or (current - start) > right:
        stopping_condition.set()
        


#input:
#   (integer) 𝐷: the number of rows of the count sketch
#   (integer) 𝑊: the number of columns of the count sketch
#   (integer) left: the left endpoint of the interval of interest
#   (integer) right: the right endpoint of the interval of interest
#   (integer) 𝐾: the number of top frequent items of interest
#   (integer) portExp: the port number
if __name__ == '__main__':
    assert len(sys.argv) == 7, "USAGE: D W left right K portExp"
    conf = SparkConf().setMaster("local[*]").setAppName("G078HW3")
    #conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")    #only if out of memory error
    
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    
    print("\nINFO:")

    D = int(sys.argv[1])
    W = int(sys.argv[2])
    print("Count Sketch: {0}x{1}".format(D, W))

    left = int(sys.argv[3])
    right = int(sys.argv[4])
    print("Interval of interest: [{0},{1}]".format(left, right))

    K = int(sys.argv[5])
    print("Top frequent items of interest:", K)

    portExp = int(sys.argv[6])
    print("Receiving data from port:", portExp)
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    
    C = [([0] * W) for i in range(D)]   #counters matrix for Count Sketch

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    
    initialTime = datetime.datetime.now()
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("\nSTARTING streaming engine\n")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("\nSTOPPING the streaming engine\n")
    ssc.stop(False, True)
    print("\nStreaming engine STOPPED\n")

    # COMPUTE AND PRINT FINAL STATISTICS
    print("Number of items processed =", streamLength[0])
    print("Number of distinct items =", len(histogram))
    largest_item = max(histogram.keys())
    print("Largest item =", largest_item)
    print("Frequency of {0} = {1}".format(largest_item, histogram[largest_item]))

    #Exact F_2
    F_1 = 0
    F_2 = 0
    for key in histogram.keys():
        F_1 += histogram[key]
        F_2 += histogram[key]**2

    F_2 = F_2/(F_1**2)

    print("F_1:", F_1)
    print("Exact F_2 (normalized):", F_2)

    #Approximate F_2



