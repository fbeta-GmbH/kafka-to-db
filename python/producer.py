from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random 
import os
from os.path import join, dirname
import time
from dotenv import load_dotenv
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

fake=Faker()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

p=Producer({
    'bootstrap.servers':'localhost:9092',
    'linger.ms': 10,
    'acks': 1,
    })
print('Kafka Producer has been initiated...')

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

def main():
    for i in range(10):
        print(f'Producing message {i} ...')
        data={
           'test':i  
           }
        print('dumping data')
        m=json.dumps(data)
        print('polling')
        p.poll()
        print('producing')
        p.produce('user-tracker', m.encode('utf-8'),callback=receipt)
        print('flushing')
        p.flush()
        print('sleeping for 3 seconds...')
        time.sleep(3)

if __name__ == '__main__':
    main()
    print('Producer has been terminated...')
