import os
import kafka_helper
import json

topic = "{}temp".format(os.environ["KAFKA_PREFIX"])

print ("START! {}".format(topic))

producer = kafka_helper.get_kafka_producer()

index = 0
while index < 200000:
    producer.send(topic, value={"temp":index})
    print ("SENT {}!".format(index))
    index+=1

consumer = kafka_helper.get_kafka_consumer(topic=topic)
print ("Connected")
for message in consumer:
    print (message)
