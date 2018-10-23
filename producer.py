import threading
import logging
import time
import multiprocessing
import os
import kafka_helper
from sense_hat import SenseHat
from dotenv import load_dotenv
# from kafka import KafkaConsumer, KafkaProducer

sense = SenseHat()

print (os.environ["ENV"])

if os.environ['ENV'] != 'production':
    load_dotenv()

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = kafka_helper.get_kafka_producer()

        while not self.stop_event.is_set():
            raw_temp = sense.get_temperature()
            temp = round(raw_temp-10.0, 1)
               
            raw_accel = sense.get_accelerometer()
            p = round(raw_accel['pitch'], 6)
            r = round(raw_accel['roll'], 6)
            y = round(raw_accel['yaw'], 6)
            accel = { 'pitch': p, 'roll': r, 'yaw': y }
               
            raw_pressure = sense.get_pressure()
            pressure = round(raw_pressure, 0)
               
            raw_humidity = sense.get_humidity()
            humidity = round(raw_humidity, 1)
               
            producer.send('temp', value=temp)
            print ("Sent temp: {}".format(temp))
            producer.send('accel', value=accel)
            print ("Sent accel: {p}, {r}, {y}".format(accel))
            producer.send('pressure', value=pressure)
            print ("Sent pressure: {}".format(pressure))
            producer.send('humidity', value=humidity)
            print ("Sent humidity: {}".format(humidity))
               
            time.sleep(0.1)

        producer.close()


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = kafka_helper.get_kafka_consumer(topic='temp')
        # consumer.subscribe(['temp'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    tasks = [
        Producer(),
        Consumer()
    ]

    for t in tasks:
        t.start()

    time.sleep(10)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()
