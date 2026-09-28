import argparse
from uuid import uuid4
import pandas as pd
import importlib
import dbutils
from datetime import datetime
import logging
importlib.reload(dbutils)
import configuration,time
importlib.reload(configuration)
from configuration import kafka_utils as ku,Car
from dbutils import Dbutils as db
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer


logger=logging.getLogger(__file__)
logfile_nm=str(__file__)[:-3]+'_'+str(datetime.now().strftime('%Y%m%d-%H%M%S'))
logging.basicConfig(filename=f'{logfile_nm}.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filemode='w')

logger.setLevel(logging.INFO)

prev_date=datetime(2024,1,8)

    
def car_to_dict(car:Car, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return car.record

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        logger.info(f"Delivery failed for User record {msg.key()}: {err}")
        return
    logger.info(f'User record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
db_obj=db()
db_obj.db_connect()

def get_car_instance():
    query="select max(Record_ld_dt) as max_record_ld_dts from kafka_test.cardekho_sales_dtl"
    max_date=db_obj.run_query(query)['max_record_ld_dts'].values[0]
    global prev_date
    '''if max_date==prev_date:
        logger.info("No New data found")
        return -1'''
    count=0
    while(max_date==prev_date and count<=10):
        logger.info("No New data found")
        logger.info(f"Trying to fetch data for {count + 1} time.")
        time.sleep(1)
        query="select max(Record_ld_dt) as max_record_ld_dts from kafka_test.cardekho_sales_dtl"
        max_date=db_obj.run_query(query)['max_record_ld_dts'].values[0]
        count=count+1
    if count>10:
        logger.info("No. of max retries has been reached.")
        yield list([False])
    
    logger.info("New records are present in the table")
    query=f"select * from kafka_test.cardekho_sales_dtl where Record_ld_dt='{max_date}'"
    df=db_obj.run_query(query)
    df['Record_ld_dt']=df['Record_ld_dt'].astype(str)
    prev_date=max_date
    for data in df.values:
        car=Car(dict(zip(df.columns,data)))
        yield car

def producer_main(topic):
    schema_registry_conf = ku.schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    logger.info("Schema registry client has been initialized.")
    # subjects = schema_registry_client.get_subjects()
    # print(subjects)
    subject = topic+'-value'
    logger.info(f"Subject : {subject}")

    schema = schema_registry_client.get_latest_version(subject)
    schema_str=schema.schema.schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, car_to_dict)
    logger.info("Serializers has been initialized for Key and Value")

    producer = Producer(ku.sasl_conf())

    logger.info(f"Producing user records to topic {topic}.")
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        bool=True
        while(bool):
            for car in get_car_instance():
                logger.info(f'car is {car}')
                if type(car) is list and car[0] is False:
                    logger.info("Exiting the Producer Loop.")
                    bool=False
                    break
                logger.info("Producing to kafka cluster.")
                producer.produce(topic=topic,
                                key=string_serializer(str(uuid4())),
                                value=json_serializer(car, SerializationContext(topic, MessageField.VALUE)),
                                on_delivery=delivery_report)
            if bool==False:
                break
            
    except KeyboardInterrupt:
        pass
    except ValueError:
        logger.info("Invalid input, discarding record...")
        pass

    logger.info("Flushing records...")
    producer.flush()

if __name__=='__main__':
    producer_main("car_topic")