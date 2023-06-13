from xml.etree.ElementTree import tostring
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
from statistics import median

# After how many items should we stop?
THRESHOLD = 10000000

#Count Sketch hash function h: U -> {0,1,...,W-1}
def hash1(key, row):
    #(row+1) to not have always hash = 0 in row 0
    h = (key * (row+1)) % W

    return h

#Count Sketch hash function g: U -> {-1, +1}
def hash2(key, row):
    g = (key * (row+1)) % W

    if (g % 2) == 1:
        return 1
    else:
        return -1

def findTopKItems(hist, k):
    topK = [0] * k      #list with top-K items with highest value
    for i in range(k):
        items = hist.items()    #list with tuples in hist
        max_val = 0     #current maximum value
        max_key = 0     #key associated to current max
        
        for elem in items:
            #if equal values take the one with greater key
            if elem[1] == max_val:
                if elem[0] > max_key:
                    max_key = elem[0]

            #if current elem analyzed has value greater than max_val, update max_val and max_key
            if elem[1] > max_val:
                max_key = elem[0]
                max_val = elem[1]

        topK[i] = (max_key, max_val)
        hist.pop(max_key)

    return topK

# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):

    global streamLength, histogram, C
    batch_size = batch.count()

    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size

    batch_items = (batch.filter(lambda x: int(x) in range(left, right+1))
        .map(lambda s: (int(s), 1))
        .groupByKey()
        .map(lambda x: (x[0], sum(x[1]))))

    for t in batch_items.collect():

        key = t[0]

        #Exact computation
        if key not in histogram:
            histogram[key] = t[1]
        else:
            histogram[key] += t[1]

        #Count Sketch
        for l in range(t[1]):       #for every time that item key appears in the stream    
            for i in range(D):      #for every row
                C[i][hash1(key, i)] += hash2(key, i)

    if streamLength[0] >= THRESHOLD:
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
   
    D = int(sys.argv[1])
    W = int(sys.argv[2])

    left = int(sys.argv[3])
    right = int(sys.argv[4])

    K = int(sys.argv[5])
    assert K <= (right-left+1), "K cannot be greater than the number of distinct elements we have."

    portExp = int(sys.argv[6])
    
    output = "D = {0} W = {1} [left,right] = [{2},{3}] K = {4} Port = {5}\n". format(D,W,left,right,K,portExp)

    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0]  # Stream length (an array to be passed by reference)
    histogram = {}  # Hash Table for the distinct elements
    C = [([0] * W) for i in range(D)]   #counters matrix for Count 

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    ssc.start()

    stopping_condition.wait()

    ssc.stop(False, True)

    # COMPUTE AND PRINT FINAL STATISTICS
    largest_item = max(histogram.keys())

    #Exact F_1 and F_2
    F_1 = 0     #|SIGMA_R|
    F_2 = 0     #SECOND MOMENT
    for key in histogram.keys():
        F_1 += histogram[key]
        F_2 += histogram[key]**2

    F_2 = F_2/(F_1**2)

    output += "Total number of items = {0} \n".format(streamLength[0])

    output += "Total number of items in [{0},{1}] = {2}\n".format(left,right,F_1)

    output += "Number of distinct items in [{0},{1}] = {2}\n".format(left,right,len(histogram))

    #Approximate F_2
    F_2_tilde = [0] * D
    for j in range(D):
        for k in range(W):
            F_2_tilde[j] += ((C[j][k])**2)        
    
    F_2_CS = median(F_2_tilde)/(F_1**2)

    #Average relative error of frequency estimates
    avg_err = 0

    #find the top-K's fu components
    kLargest_fu = findTopKItems(histogram, K)

    #find the K's fu_tilde components
    kLargest_fu_tilde = [0]*K
    for i in range(K):
        element_u = kLargest_fu[i][0]
        for_medians = [0] * D
        for j in range(D):
            for_medians[j] = C[j][hash1(element_u,j)] * hash2(element_u, j)
        kLargest_fu_tilde[i] = median(for_medians)

    #computing avg error by summing all |fu - fu_tilde| / fu components and dividing them by K
    for i in range(K):
        avg_err += (abs(kLargest_fu[i][1]-kLargest_fu_tilde[i]))/kLargest_fu[i][1]
    avg_err = avg_err/K

    if K<=20:
        for i in range(K):
            output += "Item {0} Freq = {1} Est. Freq = {2}\n".format(kLargest_fu[i][0], kLargest_fu[i][1], kLargest_fu_tilde[i])

    output += "Avg err for top {0} = {1}\n".format(K,avg_err)
    output += "F2 {0} F2 Estimate {1}\n".format(F_2,F_2_CS)
    
    print(output)

