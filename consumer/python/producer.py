from confluent_kafka import Producer
from faker import Faker
import json
import time
import logging
import random 
import os
from os.path import join, dirname
import time

fake=Faker()

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

p=Producer({
    'bootstrap.servers':os.getenv('KAFKA_BROKERS'),
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
        key = fake.uuid4()
        headers = { }
        for i in range(random.randint(1,5)):
            headers[fake.word()] = fake.word()
        data={
           'test':random.randint(1,100)  
           }
        m=json.dumps(data)
        p.poll(3)
        p.produce(os.getenv('KAFKA_TOPIC'), key=key, headers = headers, value= m.encode('utf-8'),callback=receipt)
        p.flush(3)
        time.sleep(3)

if __name__ == '__main__':
    main()
