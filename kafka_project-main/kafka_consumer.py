import argparse
from uuid import uuid4
import pandas as pd
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka import Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import importlib
import configuration
importlib.reload(configuration)
from configuration import kafka_utils as ku,Car
from datetime import datetime
import logging

logger=logging.getLogger(__file__)
logfile_nm=str(__file__)[:-3]+'_'+str(datetime.now().strftime('%Y%m%d-%H%M%S'))
logging.basicConfig(filename=f'{logfile_nm}.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filemode='w')

logger.setLevel(logging.INFO)
    
def consumer_main(topic):

    schema_registry_conf = ku.schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    logger.info("Schema registry client has been initialized.")
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'
    logger.info(f"Subject : {subject}")

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Car.dict_to_car)
    logger.info("De-Serializer has been initialized for Value")

    consumer_conf = ku.sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    logger.info("Consumer has been initialized.")


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                logger.info("Not enough messages to consume")
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if car is not None:
                logger.info(f"User record {msg.key()}: car: {car}")
        except KeyboardInterrupt:
            break

    consumer.close()

consumer_main("car_topic")