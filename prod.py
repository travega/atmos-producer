from sense_hat import SenseHat
import kafka
import kafka_helper
import time
import os

sense = SenseHat()
sense.set_imu_config(False, False, True)

#producer = HerokuKafkaProducer(
#            url=os.environ['KAFKA_URL'],
#            ssl_cert=os.environ['KAFKA_CLIENT_CERT'],
#            ssl_key=os.environ['KAFKA_CLIENT_CERT_KEY'],
#            ssl_ca=os.environ['KAFKA_TRUSTED_CERT'],
#            prefix=os.environ['KAFKA_PREFIX']
#        )

producer = kafka_helper.get_kafka_producer()

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

    producer.send('temp', value=temp)
    print ("Sent temp: {}".format(temp))
    producer.send('accel', value=accel)
    print ("Sent accel: {p}, {r}, {y}".format(accel))
    producer.send('pressure', value=pressure)
    print ("Sent pressure: {}".format(pressure))
    producer.send('humidity', value=humidity)
    print ("Sent humidity: {}".format(humidity))

    producer.flush()

    time.sleep(0.1)
