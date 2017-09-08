import json
import csv
import threading, logging, time
from kafka import KafkaProducer, KafkaConsumer

def get_producer(host,port):
    try:
        producer = KafkaProducer(bootstrap_servers='%s:%s'%(str(host),str(port)),retries=5)
        return True, producer
    except Exception,e:
        print e
        return False, ""

def send_message(message, topic):
    print "*"*10
    print "Sending message: %s"%str(message)
    print "Topic: %s"%topic
    print "*"*10
    try:
        producer.send(topic, json.dumps(message))
    except Exception,e:
        try:
            future = producer.send(topic, json.dumps(message))
            result = future.get(timeout=60)
        except Exception,e:
            print e
    producer.flush()

def get_kafka_messages(path):
    with open(path, "rb") as f:
        reader = csv.reader(f, delimiter="\t")
        for i, line in enumerate(reader):
            record = line[0].split(',')
            if len(record) == 4:
                record = [record[0]+record[1], record[2], record[3]]
            yield {'CompanyName':record[0], 'FirstName':record[1], 'LastName':record[2]}


if __name__ == "__main__":
    host, port = "localhost", "9092"
    status, producer = get_producer(host,port)
    path, topic,csv_len = "/home/pooja/pyspark_examples/csv_files/Aug28.csv", "file_upload", 2
    if status:
        message_iterator = iter(get_kafka_messages(path))
        for each in range(csv_len):
            try:
                send_message(message_iterator.next(), topic)
            except StopIteration:
                pass
    else:
        print "error in connecting kafka"
