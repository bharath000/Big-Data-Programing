import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf

# creating sprak session and config
conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)

#reading the tweets and city map json files using spark.read.json  build commmand as dataframe
tweets  = spark.read.json("/home/vmalapati1/data/tweets.json")
citymap  = spark.read.json("/home/vmalapati1/data/cityStateMap.json")

"""
joining the above dataframes using condition if geo and city cloumns are same or join by same city
and after joining it consists of two columns with city names and drop one of them
"""
joined = tweets.join(citymap, tweets["geo"] == citymap["city"]).drop('city')

#After joing the data frames as now compute the cities in states using \
# grupby by states and counting the number
j1 = joined.groupBy("state").count()

#displaying the results

tweets.show()   #displaying tweets dataframe
citymap.show()  #dispalying citymap dataframe
joined.show()  # joined tweets and citymap data frame
j1.show()       # j1 compute the unique counts dataframe