import argparse
from datetime import datetime
import importlib
import logging

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)

import configuration

importlib.reload(configuration)
from configuration import Car
from configuration import kafka_utils as ku

# Configure logging
logger = logging.getLogger(__file__)
logfile_nm = (
    str(__file__)[:-3] + '_' + str(datetime.now().strftime('%Y%m%d-%H%M%S'))
)
logging.basicConfig(
    filename=f'{logfile_nm}.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filemode='w',
)
logger.setLevel(logging.INFO)


# Token refresh callback required by confluent-kafka for MSK IAM Auth
def oauth_cb(oauth_config):
  auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
      'ap-south-1'
  )
  return auth_token, expiry_ms / 1000  # Converts ms to seconds for librdkafka


def consumer_main(topic):
  # 1. Initialize Schema Registry Client
  schema_registry_conf = ku.schema_config()
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  logger.info('Schema registry client has been initialized.')

  # 2. Fetch latest schema for topic
  subject = topic + '-value'
  logger.info(f'Subject : {subject}')
  schema = schema_registry_client.get_latest_version(subject)
  schema_str = schema.schema.schema_str

  # 3. Setup Deserializer
  json_deserializer = JSONDeserializer(
      schema_str, from_dict=Car.dict_to_car
  )
  logger.info('Deserializer has been initialized for Value.')

  # 4. Configure MSK Consumer Configuration
  consumer_conf = {
      'bootstrap.servers': (
          'boot-map86xor.c2.kafka-serverless.ap-south-1.amazonaws.com:9098'
      ),
      'security.protocol': 'SASL_SSL',
      'sasl.mechanism': 'OAUTHBEARER',
      'oauth_cb': oauth_cb,
      'group.id': 'car-consumer-group-1',  # Unique consumer group ID
      'auto.offset.reset': 'earliest',
  }

  consumer = Consumer(consumer_conf)
  consumer.subscribe([topic])
  logger.info('Consumer connected and subscribed to topic.')

  # 5. Start Polling Loop
  while True:
    try:
      msg = consumer.poll(1.0)
      if msg is None:
        logger.info('Waiting for messages...')
        continue

      if msg.error():
        logger.error(f'Consumer error: {msg.error()}')
        continue

      # Deserialize message payload using Schema Registry
      car = json_deserializer(
          msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
      )

      if car is not None:
        logger.info(f'User record key {msg.key()}: car: {car}')
        print(f'Received Car Record: {car}')

    except KeyboardInterrupt:
      logger.info('Shutdown signal received.')
      break

  consumer.close()
  logger.info('Consumer connection closed cleanly.')


if __name__ == '__main__':
  consumer_main('car_topic')