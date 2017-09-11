#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.functions import col

import functools
from tornado import ioloop, httpclient
from tornado.httpclient import AsyncHTTPClient

pendingEmailResponseCount = 0

checkpointDirectory = './data'

topic = 'file_upload'
zkQuorum = 'localhost:2181'
application_name = 'Pyspark_application'
stream_period = 10

fields = ['companyname', 'firstname', 'lastname']
schema_fields = ['companyname', 'firstname', 'lastname', 'emailpattern'
                 , 'domainname']

schema = StructType([StructField(field, StringType(), True)
                    for field in fields])

db_schema = StructType([StructField(field, StringType(), True)
                       for field in schema_fields])

conf = SparkConf().setAppName(application_name)
conf.set('spark.streaming.receiver.writeAheadLog.enable', 'true')
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, stream_period)
ssc.checkpoint(checkpointDirectory)


def getSparkSessionInstance(sparkConf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = \
            SparkSession.builder.config(conf=sparkConf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def process_df(request_dstream):
    try:
        spark = \
            getSparkSessionInstance(request_dstream.context.getConf())
        request_data = \
            spark.createDataFrame(request_dstream.flatMap(lambda s: \
                                  parse(s, fields)), schema)
        request_data.collect()
        df = spark.read.format('org.apache.spark.sql.cassandra'
                               ).options(table='getleads',
                keyspace='spark_requests').load()
        if len(request_data.collect()):
            new_requests = request_data.join(df,
                    request_data['companyname'] == df['companyname'],
                    'left_outer').select(request_data['companyname'],
                    request_data['firstname'], request_data['lastname'])
            new_requests.foreach(process_domain_requests)
    except Exception, e:
        print e
        pass


def update_domain_results(domainName, companyDetails):
    spark = getSparkSessionInstance(conf)
    df = spark.read.format('org.apache.spark.sql.cassandra'
                           ).options(table='domain_requests1',
            keyspace='spark_requests').load()
    dbDataIfExists = df.where((col('companyname')
                              == companyDetails['companyname'])
                              & (col('firstname')
                              == companyDetails['firstname'])
                              & (col('lastname')
                              == companyDetails['lastname']))
    dbDataIfExists.show()
    if dbDataIfExists.count() == 0:
        companyDetailsDict = companyDetails.asDict()
        cleanCompanyDetailsDict = {}
        for eachkey in companyDetailsDict.keys():
            cleanCompanyDetailsDict[eachkey] = \
                str(companyDetailsDict[eachkey])
        cleanCompanyDetailsDict = Row(domainname=str(domainName),
                **cleanCompanyDetailsDict)
        new_request_data = \
            spark.createDataFrame([cleanCompanyDetailsDict])

        new_request_data.write.format('org.apache.spark.sql.cassandra'
                ).mode('append').options(table='domain_requests1',
                keyspace='spark_requests').save()
    else:
        dbDataIfExists = df.withColumn('domainname',
                lit(str(domainName)))

        dbDataIfExists.write.format('org.apache.spark.sql.cassandra'
                                    ).mode('append'
                ).options(table='domain_requests1',
                          keyspace='spark_requests').save()


def handle_response(companyDetails, response):
    global pendingEmailResponseCount
    print type(companyDetails)
    pendingEmailResponseCount -= 1
    if response.error:
        print 'Error: %s' % response.error
    else:
        print 'Response >> ' + str(response.body)
        domain_response = json.loads(response.body)
        for each in domain_response['result']:
            update_domain_results(each['domainName'], companyDetails)
    if pendingEmailResponseCount == 0:
        ioloop.IOLoop.instance().stop()


def process_domain_requests(companyDetails):
    import urllib
    global pendingEmailResponseCount
    http_client = AsyncHTTPClient()
    callback = functools.partial(handle_response, companyDetails)
    quoted_query = urllib.quote(companyDetails['companyname'])
    http_client.fetch('http://localhost:5000/domain/%s'
                      % str(quoted_query), callback)
    pendingEmailResponseCount += 1
    ioloop.IOLoop.instance().start()


def parse(s, fields):
    try:
        d = json.loads(s[1])
        return [tuple(str(d.get(field)).strip() for field in fields)]
    except:
        return []


if __name__ == '__main__':
    kafkaStream = KafkaUtils.createStream(ssc, zkQuorum,
            'spark-cassandra', {topic: 1})
    kafkaStream.pprint()
    kafkaStream.foreachRDD(process_df)
    ssc.start()
    ssc.awaitTermination()
