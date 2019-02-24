import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    posCount = []
    negCount = []
    for count in counts:
        if count[0][0] == 'positive':
            posCount.append(count[0][1])
            negCount.append(count[1][1])
        else:
            posCount.append(count[1][1])
            negCount.append(count[0][1])
    plt.xlabel("Time Step")
    plt.ylabel("Word Count")
    plt.plot(posCount, '-o', label='positive')
    plt.plot(negCount, '-o', label='negative')
    plt.legend()
    plt.show()



def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    return set([x.strip() for x in open(filename, "r")])


def updateCount(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream( ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    tweets = tweets.flatMap(lambda tweet: tweet.split(" ")).\
        filter(lambda word: word in pwords or word in nwords).\
	map(lambda word: ('positive', 1) if word in pwords else ('negative', 1)).\
        reduceByKey(lambda x, y: x+y)
    runningCount = tweets.updateStateByKey(updateCount)
    runningCount.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    tweets.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts


if __name__=="__main__":
    main()
