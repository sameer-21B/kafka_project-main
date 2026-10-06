from datetime import datetime
import importlib
import logging
import time

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
import redshift_connector

import configuration

importlib.reload(configuration)
from configuration import Car
from configuration import kafka_utils as ku

# Logging Setup
logger = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

# Redshift Connection Settings
# REDSHIFT_DETAILS = {
#     'host': 'default-workgroup.243345108289.ap-south-1.redshift-serverless.amazonaws.com',
#     'database': 'dev',
#     'port': 5439,
#     'user': 'admin',
#     'password': 'NSSQLzdyxz818(',
# }

REDSHIFT_DETAILS = {
    'host': 'default-workgroup.243345108289.ap-south-1.redshift-serverless.amazonaws.com', # Replace with your Workgroup Endpoint
    'database': 'dev',
    'port': 5439,
    'iam': True,
    'region': 'ap-south-1',
    'db_user': 'awsuser'                  # Default serverless admin user
}


def oauth_cb(oauth_config):
  auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(
      'ap-south-1'
  )
  return auth_token, expiry_ms / 1000


def flush_batch_to_redshift(batch_records):
  """Inserts accumulated records into Redshift using bulk insert."""
  if not batch_records:
    return

  logger.info(
      f'Flushing {len(batch_records)} records to Redshift database...'
  )

  try:
    conn = redshift_connector.connect(**REDSHIFT_DETAILS)
    cursor = conn.cursor()

    # Prepare SQL statement for batch insertion
    query = """
            INSERT INTO cars (Idx, Brand, Car_name, Engine, Fuel_type, Km_driven, Max_power, Mileage,Model,Record_ld_dt,Seats,Seller_type,Transmission_type,Vehicle_age) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

    # Transform Car objects into tuple lists
    data_tuples = [
        (c.Idx, c.Brand, c.Car_name, c.Engine, c.Fuel_type, c.Km_driven, c.Max_power, c.Mileage, c.Model, c.Record_ld_dt, c.Seats, c.Seller_type, c.Transmission_type, c.Vehicle_age) for c in batch_records
    ]

    cursor.executemany(query, data_tuples)
    conn.commit()

    logger.info(
        f'Successfully written {len(batch_records)} records to Redshift.'
    )
    cursor.close()
    conn.close()

  except Exception as e:
    logger.error(f'Failed to insert batch into Redshift: {e}')


def consumer_main(topic):
  # 1. Schema Registry setup
  schema_registry_conf = ku.schema_config()
  schema_registry_client = SchemaRegistryClient(schema_registry_conf)
  schema = schema_registry_client.get_latest_version(topic + '-value')
  json_deserializer = JSONDeserializer(
      schema.schema.schema_str, from_dict=Car.dict_to_car
  )

  # 2. MSK Kafka Consumer Setup
  consumer_conf = {
      'bootstrap.servers': (
          'boot-map86xor.c2.kafka-serverless.ap-south-1.amazonaws.com:9098'
      ),
      'security.protocol': 'SASL_SSL',
      'sasl.mechanism': 'OAUTHBEARER',
      'oauth_cb': oauth_cb,
      'group.id': 'redshift-batch-group',
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': False,  # Manual commit after Redshift write
  }

  consumer = Consumer(consumer_conf)
  consumer.subscribe([topic])

  # Batch Control Variables
  batch_buffer = []
  last_flush_time = time.time()
  BATCH_INTERVAL_SECONDS = 60

  logger.info(
      f'Starting Redshift batch consumer (Interval: {BATCH_INTERVAL_SECONDS}s)...'
  )

  try:
    while True:
      msg = consumer.poll(1.0)
      current_time = time.time()

      if msg is not None and not msg.error():
        # Parse payload
        car = json_deserializer(
            msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
        )
        if car:
          batch_buffer.append(car)

      # Check if 60 seconds have elapsed
      if (current_time - last_flush_time) >= BATCH_INTERVAL_SECONDS:
        if batch_buffer:
          flush_batch_to_redshift(batch_buffer)
          consumer.commit(asynchronous=False)  # Commit Kafka offsets
          batch_buffer.clear()
        else:
          logger.info('60s elapsed: No new messages to flush.')

        last_flush_time = current_time

  except KeyboardInterrupt:
    logger.info('Shutting down consumer...')
  finally:
    # Flush remaining records before exiting
    if batch_buffer:
      flush_batch_to_redshift(batch_buffer)
      consumer.commit(asynchronous=False)
    consumer.close()


if __name__ == '__main__':
  consumer_main('car_topic')