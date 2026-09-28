import argparse
from uuid import uuid4
import pandas as pd
import importlib
import dbutils
from datetime import datetime
import logging
importlib.reload(dbutils)
import configuration, time
importlib.reload(configuration)
from configuration import kafka_utils as ku, Car
from dbutils import Dbutils as db

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer

# AWS MSK IAM Signer Library
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

# Initialize Logger
logger = logging.getLogger(__file__)
logfile_nm = str(__file__)[:-3] + '_' + str(datetime.now().strftime('%Y%m%d-%H%M%S'))
logging.basicConfig(
    filename=f'./Logs/{logfile_nm}.log',
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger.setLevel(logging.INFO)

prev_date = datetime(2024, 1, 8)

# Set your AWS Region where MSK is hosted
AWS_REGION = 'us-east-1' 

def oauth_cb(config_str):
    """
    Callback function that dynamically fetches and refreshes 
    IAM OAuth tokens for Amazon MSK.
    """
    try:
        # Generates a base64 encoded token using EC2 IAM Role credentials
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(AWS_REGION)
        # confluent_kafka expects expiry in seconds (float)
        return token, expiry_ms / 1000.0
    except Exception as e:
        logger.error(f"Error generating MSK IAM Auth token: {e}")
        raise e

def car_to_dict(car: Car, ctx):
    return car.record

def delivery_report(err, msg):
    if err is not None:
        logger.info(f"Delivery failed for User record {msg.key()}: {err}")
        return
    logger.info(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

db_obj = db()
db_obj.db_connect()

def get_car_instance():
    query = "select max(Record_ld_dt) as max_record_ld_dts from kafka_test.cardekho_sales_dtl"
    max_date = db_obj.run_query(query)['max_record_ld_dts'].values[0]
    global prev_date
    count = 0
    while max_date == prev_date and count <= 10:
        logger.info("No New data found")
        logger.info(f"Trying to fetch data for {count + 1} time.")
        time.sleep(1)
        query = "select max(Record_ld_dt) as max_record_ld_dts from kafka_test.cardekho_sales_dtl"
        max_date = db_obj.run_query(query)['max_record_ld_dts'].values[0]
        count += 1
    if count > 10:
        logger.info("No. of max retries has been reached.")
        yield [False]

    logger.info("New records are present in the table")
    query = f"select * from kafka_test.cardekho_sales_dtl where Record_ld_dt='{max_date}'"
    df = db_obj.run_query(query)
    df['Record_ld_dt'] = df['Record_ld_dt'].astype(str)
    prev_date = max_date
    for data in df.values:
        car = Car(dict(zip(df.columns, data)))
        yield car

def get_msk_producer_config():
    """
    Returns confluent-kafka Producer configurations suited for AWS MSK IAM authentication.
    """
    return {
        # Replace with your MSK Bootstrap Brokers (IAM Endpoint, Port 9098)
        'bootstrap.servers': 'boot-map86xor.c2.kafka-serverless.ap-south-1.amazonaws.com:9098',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': oauth_cb
    }

def producer_main(topic):
    # Ensure ku.schema_config() returns your accessible Schema Registry endpoint
    schema_registry_conf = ku.schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    logger.info("Schema registry client has been initialized.")

    subject = topic + '-value'
    logger.info(f"Subject : {subject}")

    schema = schema_registry_client.get_latest_version(subject)
    schema_str = schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict)
    logger.info("Serializers have been initialized for Key and Value")

    # Pass the updated AWS MSK IAM configurations to Producer
    producer_config = get_msk_producer_config()
    producer = Producer(producer_config)

    logger.info(f"Producing user records to topic {topic}.")
    producer.poll(0.0)
    try:
        is_running = True
        while is_running:
            for car in get_car_instance():
                logger.info(f'car is {car}')
                if isinstance(car, list) and car[0] is False:
                    logger.info("Exiting the Producer Loop.")
                    is_running = False
                    break
                logger.info("Producing to kafka cluster.")
                producer.produce(
                    topic=topic,
                    key=string_serializer(str(uuid4())),
                    value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)),
                    on_delivery=delivery_report
                )
            if not is_running:
                break

    except KeyboardInterrupt:
        pass
    except ValueError:
        logger.info("Invalid input, discarding record...")
        pass

    logger.info("Flushing records...")
    producer.flush()

if __name__ == '__main__':
    producer_main("car_topic")