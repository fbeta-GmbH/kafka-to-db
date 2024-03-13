import os
from os.path import join, dirname
import time
import logging
# from dotenv import load_dotenv
# dotenv_path = join(dirname(__file__), '.env')
# load_dotenv(dotenv_path)
time.sleep(10)
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='producer.log',
                    # filemode='w'
                    )

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, TEXT, JSON, TIMESTAMP, ForeignKey
from sqlalchemy import create_engine

Base = declarative_base()

class Headers(Base):
    __tablename__ = 'headers'
    id = Column(Integer, primary_key=True)
    key = Column(TEXT)
    value = Column(TEXT)
    data_id = Column(Integer, ForeignKey('data.id'))
    data = relationship("Data", back_populates="headers")

class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    key = Column(TEXT)
    value = Column(JSON)
    timestamp = Column(TIMESTAMP, server_default="now()")
    offset = Column(Integer)
    topic = Column(TEXT)
    headers = relationship("Headers", back_populates="data")

logger.info("Connecting to database")
engine = create_engine(os.getenv('DATABASE_URL'))
Base.metadata.create_all(engine)
# connection = engine.connect()

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

logger.info("Setting up Kafka consumer")
from confluent_kafka import Consumer

conf = {'bootstrap.servers': os.getenv('KAFKA_BROKERS'),
        'group.id': os.getenv('KAFKA_GROUP_ID'),
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

from confluent_kafka import KafkaError, KafkaException
import sys
import json

def msg_process(msg):
    logger.info('%% %s [%d] at offset %d with key %s:\n' %
          (msg.topic(), msg.partition(), msg.offset(), str(msg.key())))
    logger.info(msg.value())

    data = Data(
        key=str(msg.key().decode('utf-8')) if msg.key() else None,
        offset=msg.offset(),
        topic=msg.topic(),
        value=json.dumps(msg.value().decode('utf-8'))
        )
    session.add(data)
    session.commit()
    if msg.headers() : 
        for h in msg.headers():
            head = Headers(
                key = h[0],
                value = h[1].decode('utf-8')
            )
            head.data = data
            session.add(head)
        session.commit()

running = True

def basic_consume_loop(consumer, topics):
    try:
        logger.info("Subscribing to topics: {}".format(topics))
        consumer.subscribe(topics)

        while running:
            logger.debug("polling")
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.error('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                logger.debug('Received message: {}'.format(msg))
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False
    
if __name__ == '__main__':
    basic_consume_loop(consumer, [os.getenv('KAFKA_TOPIC')])