from graphframes import GraphFrame
from pyspark import SparkContext

from pyspark.sql import SQLContext
sc = SparkContext('local')
sqlContext =SQLContext(sc)

#pdb.set_trace()
df_station = sqlContext.read.csv("meta-members.csv", inferSchema = True, header = True)
df_trip = sqlContext.read.csv("member-edges.csv", inferSchema = True, header = True)


g=GraphFrame(df_station,df_trip)

# Query: Get in-degree of each vertex.
g.inDegrees.show()
g.outDegrees.show()

g.find("(2069)-[]->(211406247)").show()
results = g.pageRank(sourceId=2069,resetProbability=0.1, maxIter=20)
results.vertices.select("id", "pagerank").show()