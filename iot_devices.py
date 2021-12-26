#!/usr/bin/python3

from kafka import KafkaProducer
import numpy as np
from sys import argv, exit
from time import time, sleep

# Different device "profiles" with different
# distributions of values to make things interesting
DEVICES_PROFILES = {
    "Boston": {'temp': (51.3, 17.7), 'humd': (77.4, 18.7), 'pres': (1019.9, 9.5)},
    "Dakar": {'temp': (29.3, 24.7), 'humd': (33.4, 13.9), 'pres': (1012.4, 43.5)},
    "Paris": {'temp': (11.3, 5.7), 'humd': (62.2, 24.2), 'pres': (1015.6, 13.5)},
}

# To run the script, we've to give an argument
# (the device profile or profile name). Eg. Dakar
if len(argv) != 2 or argv[1] not in DEVICES_PROFILES :
    print("Please, provide a valid device name :")
    for key in DEVICES_PROFILES.keys() :
        print(f" * {key}")
    print(f"\nformat: {argv[0]} DEVICE_NAME")
    exit(1)

profile_name = argv[1]
profile = DEVICES_PROFILES[profile_name]

# Kafka Porducer
producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True :
    temp = np.random.normal(profile['temp'][0], profile['temp'][1])
    humd = max(0, min(np.random.normal(profile['humd'][0], profile['humd'][1], 100)))
    pres = np.random.normal(profile['pres'][0], profile['pres'][1])

    # Formating data (event)
    msg = f'{time()}, {profile_name}, {temp}, {humd}, {pres}'

    # Seding event to the topic WEATHER
    producer.send('weather', bytes(msg, encoding='utf8'))
    print('Sending data to kafka')

    sleep(0.5)