import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

# Load the adjacency list file
AdjList1 = sc.textFile("/home/vmalapati1/data/02AdjacencyList.txt")
print (AdjList1.collect())
#['1 2', '2 3 4', '3 4', '4 1 5', '5 3']

AdjList2 = AdjList1.map(lambda line : line.split(' '))\
    .flatMap(lambda token: [(int(i),[float(token[0]), 1/(len(token)-1)]) for i in token[1:]]) # 1. Replace the lambda function with yours
print(AdjList2.collect())
#[(2, [1.0, 1.0]), (3, [2.0, 0.5]), (4, [2.0, 0.5]), (4, [3.0, 1.0]), (1, [4.0, 0.5]), (5, [4.0, 0.5]), (3, [5.0, 1.0])]


AdjList3 = AdjList2.groupByKey().sortByKey().map(lambda x: [x[0], [v for v in x[1]]])

print(AdjList3.collect())
#[[1, [[4.0, 0.5]]], [2, [[1.0, 1.0]]], [3, [[2.0, 0.5], [5.0, 1.0]]], [4, [[2.0, 0.5], [3.0, 1.0]]], [5, [[4.0, 0.5]]]]

num_Nodes = AdjList3.count()
print("Total Number of nodes")
print(AdjList3.count())



PageRankValues = AdjList3.mapValues(lambda x : 0.20000)
print(PageRankValues.collect())
#[(1, 0.2), (2, 0.2), (3, 0.2), (4, 0.2), (5, 0.2)]
jRDD = AdjList3.flatMap(lambda x: [[i[0],[i[1],x[0]]] for i in x[1]])
jRDD.persist()
print(jRDD.collect())
#[[4.0, [0.5, 1]], [1.0, [1.0, 2]], [2.0, [0.5, 3]], [5.0, [1.0, 3]], [2.0, [0.5, 4]], [3.0, [1.0, 4]], [4.0, [0.5, 5]]]

for i in range(1, 31):
    joinrdd = jRDD.join(PageRankValues)
    print(joinrdd.collect())
    #[(4.0, ([0.5, 1], 0.2)), (4.0, ([0.5, 5], 0.2)), (2.0, ([0.5, 3], 0.2)), (2.0, ([0.5, 4], 0.2)), (1.0, ([1.0, 2], 0.2)), (5.0, ([1.0, 3], 0.2)), (3.0, ([1.0, 4], 0.2))]

    print(i,'iteration')
    PageRankValues = joinrdd.map(lambda x: [x[1][0][1], x[1][0][0]*x[1][1]]).\
    reduceByKey(lambda x,y: (x+y))\
    .map(lambda x: (x[0], x[1]*0.85+((1-0.85)/num_Nodes))).sortByKey()
    
          # reduceByKey(lambda x: x*0.85)
    #print(PageRankValues.collect())
    #joinrdd = jRDD.join(PageRankValues)
    #print(PageRankValues.collect())
    #print(joinrdd.collect())
    #PageRankValues = joinrdd.map(lambda x: [x[1][0][1], x[1][0][0]*x[1][1]]).reduceByKey(lambda x,y: (x+y)*0.85+((1-0.85)/5))\
           # .sortByKey()

    print(PageRankValues.collect())  
      

print ("=== Final PageRankValues ===")
print (PageRankValues.collect())
topk = PageRankValues.collect()
'''
output_filepath = '/home/vmalapati1/data/PageRankValues_Final.txt'
PageRankValues.coalesce(1).saveAsTextFile("/home/vmalapati1/data/PageRankValues_Final")
outF = open(output_filepath, "w")
for line in topk:
  # write line to output file
  outF.write(str(line))
  outF.write("\n")
outF.close()
'''
'''
AdjList3 = AdjList2.map(lambda x : x)  # 2. Replace the lambda function with yours
AdjList3.persist()
print(AdjList3.collect())

nNumOfNodes = AdjList3.count()
print("Total Number of nodes")
print(nNumOfNodes)

# Initialize each page's rank; since we use mapValues, the resulting RDD will have the same partitioner as links
print ("Initialization")
PageRankValues = AdjList3.mapValues(lambda v : v)  # 3. Replace the lambda function with yours
print (PageRankValues.collect())

# Run 30 iterations
print ("Run 30 Iterations")
for i in range(1, 30):
    print ("Number of Iterations")
    print (i)
    JoinRDD = AdjList3.join(PageRankValues)
    print ("join results")
    print (JoinRDD.collect())
    contributions = JoinRDD.flatMap(lambda x_y_z : x)  # 4. Replace the lambda function with yours
    print ("contributions")
    print (contributions.collect())
    accumulations = contributions.reduceByKey(lambda x, y : x)  # 5. Replace the lambda function with yours
    print ("accumulations")
    print (accumulations.collect())
    PageRankValues = accumulations.mapValues(lambda v : v)  # 6. Replace the lambda function with yours
    print ("PageRankValues")
    print (PageRankValues.collect())

print ("=== Final PageRankValues ===")
print (PageRankValues.collect())

# Write out the final ranks
#PageRankValues.coalesce(1).saveAsTextFile("/home/rob/Assignment4/PageRankValues_Final")
'''