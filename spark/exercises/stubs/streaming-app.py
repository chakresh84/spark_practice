import sys

# TODO: Load SparkContext and StreamingContext

if __name__ == "__main__": 
    if len(sys.argv) != 3:
        print "Usage: stateful_network_wordcount.py <hostname> <port>"
        exit(-1)
	
	# TODO: Initialize SparkContext and StreamingContext
    
    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    
    # TODO:  Write a sample wordcount program and print the output

    ssc.start()
    ssc.awaitTermination()
