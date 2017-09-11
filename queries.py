        # schema_request_data = request_data.map(lambda p: Row(companyname=p[0], firstname=p[1], lastname=p[2]))
        # schema_request_data.collect()
        # from pyspark.sql.functions import lit
        # companydetails = request_data.withColumn('domainname', lit(None).cast(StringType()))
        # companydetails = request_data.withColumn('emailpattern', lit(None).cast(StringType()))
        # valid_request_data.write.format("org.apache.spark.sql.cassandra").mode("append").options(
        #   table="domain_requests1", keyspace="spark_requests"
        # ).save()