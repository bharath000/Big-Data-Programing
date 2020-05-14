import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf
from pyspark.ml.clustering import KMeans

# IMPORTING THE KMENS AND CLUSTEREVALUTATION METRIC FROM MLLIB
from pyspark.ml.clustering import BisectingKMeans

from pyspark.ml.evaluation import ClusteringEvaluator

# SETTING UP SPARK CONTEXT AND SESSION
conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

# READING THE GIVEN DATA FILE FRONM LIBSVM FORMAT INTO DATAFRAME
dataset = spark.read.format("libsvm").load("/home/vmalapati1/data/kmeans_input.txt")

#INTIALIZING THE KMMEANS ALGO 
kmeans = BisectingKMeans(k=2, seed=1)  # 2 clusters here
# TRIANING THE MODEL WITH ABOVE DATA FRAME
model = kmeans.fit(dataset)
# PREDICTING THE RESULTS BASED ON THE INPUT OF THE MODEL
transformed = model.transform(dataset)

transformed.show(200)  

#dataset.show()
#COMPUTING THE COST OF THE KMEANS MODEL
cost = model.computeCost(dataset)
print("Within Set Sum of Squared Errors = " + str(cost))
#FINDING THE CENTERS OF THE CLUSTERS
centers = model.clusterCenters()
# PRINTING THE CENTERS OF THE CLUSTERS
for center in centers:
    
    print(center)