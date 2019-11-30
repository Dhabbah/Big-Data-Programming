import sys
import os

# os.environ["SPARK_HOME"] = "D:\\spark-2.3.1-bin-hadoop2.7\\"
# os.environ["HADOOP_HOME"]="D:\\Drive\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pdb
"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""


def main():
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
    # pdb.set_trace()
    # lines = ssc.textFileStream('log')  #'log/ mean directory name
    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    # character_counts = lines.flatMap(lambda line: line).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)
    # count_a = lines.flatMap(lambda x: x).map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b )
    print(counts)
    counts.pprint()
    # count_a.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
