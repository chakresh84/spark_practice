import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__": 
    if len(sys.argv) != 3:
        print "Usage: stateless network wordcount py file <hostname> <port>"
        exit(-1)
    sc = SparkContext(appName="PythonStreamingStatelessNetworkWordCount")
    ssc = StreamingContext(sc, 5)
    

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    running_counts = lines.flatMap(lambda line: line.split(" "))\
                          .map(lambda word: (word, 1))\
                          .reduceByKey(lambda v1,v2:v1+v2)

    running_counts.pprint()

    ssc.start()
    ssc.awaitTermination()
