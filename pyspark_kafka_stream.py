import json
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from uuid import uuid1

checkpointDirectory = "./data"

topic = 'file_upload'
zkQuorum = "localhost:2181"
application_name = 'Pyspark_application'
stream_period =10


# Get StreamingContext from checkpoint data or create a new one
def get_spark_context(application_name, stream_period, checkpointDirectory):
    conf = SparkConf().setAppName(application_name)
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, stream_period)
    ssc.checkpoint(checkpointDirectory)
    return ssc


def create_stream_kafka(ssc, topic, zkQuorum):
    return KafkaUtils.createStream(ssc, zkQuorum, "spark-cassandra", {topic: 1})


if __name__ == "__main__":
    ssc = get_spark_context(application_name, stream_period, checkpointDirectory)
    kafkaStream = create_stream_kafka(ssc, topic, zkQuorum)
    kafkaStream.pprint()
    ssc.start()
    ssc.awaitTermination()
