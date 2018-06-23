import sys

# TODO: Load SparkContext and Streaming Context

if __name__ == "__main__": 
    if len(sys.argv) != 3:
        print "Usage: stateful_network_wordcount.py <hostname> <port>"
        exit(-1)
    # TODO: Initialize SparkContext and StreamingContext
    ssc.checkpoint("checkpoint")

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    # TODO: Write a sample WordCount program and print the output using pprint()

	# TODO: Start the computation and wait for it to terminate

