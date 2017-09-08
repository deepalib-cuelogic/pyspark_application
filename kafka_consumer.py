from kafka import KafkaConsumer
consumer = KafkaConsumer('file_upload',auto_offset_reset='earliest',
		enable_auto_commit=False)
for msg in consumer:
    print (msg)
