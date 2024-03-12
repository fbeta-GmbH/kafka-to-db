import os
from os.path import join, dirname
import time
from dotenv import load_dotenv
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, JSON
from sqlalchemy import create_engine

Base = declarative_base()

class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    value = Column(JSON)

print("Connecting to database")
engine = create_engine(os.getenv('DATABASE_URL'))
Base.metadata.create_all(engine)
connection = engine.connect()

print("Setting up Kafka consumer")
from confluent_kafka import Consumer

conf = {'bootstrap.servers': os.getenv('KAFKA_BROKERS'),
        'group.id': os.getenv('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

from confluent_kafka import KafkaError, KafkaException
import sys

def msg_process(msg):
    print('%% %s [%d] at offset %d with key %s:\n' %
          (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    print(msg.value())
    connection.execute(Data.__table__.insert(), value=msg.value())

MIN_COMMIT_COUNT = 2
# Basic poll loop
running = True

def consume_loop(consumer, topics):
    try:
        print("Subscribing to topics:", topics)
        consumer.subscribe(topics)

        msg_count = 0
        while running:
            print("Polling")
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=False)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

consume_loop(consumer, [os.getenv('KAFKA_TOPIC')])