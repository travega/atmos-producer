from sense_hat import SenseHat
from dotenv import load_dotenv
import threading
import logging
import requests
import time
import json
import multiprocessing
import kafka_helper
import os
import time
import datetime

load_dotenv()
sense = SenseHat()

producer = kafka_helper.get_kafka_producer()

# CONSUMER_URL="https://atmos-consumer.herokuapp.com"
# CONSUMER_URL="https://travega.eu.ngrok.io"

class Producer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):

        while not self.stop_event.is_set():
           while True:
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
               
               t = time.time()
               ts = datetime.datetime.fromtimestamp(t).strftime('%Y%m%d%H%M%S%f')

               producer.send(self.topic("temp"), value={ "temp": temp, "timestamp": ts })
               producer.send(self.topic("accel"), value={ "accel": accel, "timestamp": ts })
               producer.send(self.topic("pressure"), value={ "pressure": pressure, "timestamp": ts })
               producer.send(self.topic("humidity"), value={ "humidity": humidity, "timestamp": ts })
               
            #    time.sleep(0.2)

    def topic(self, topic):
        return "{}{}".format(os.environ["KAFKA_PREFIX"], topic)


def main():
    tasks = [
        Producer(),
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
