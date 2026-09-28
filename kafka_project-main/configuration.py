from configparser import ConfigParser 

configure = ConfigParser()
configure.read('config.ini')

kafka_configure=ConfigParser()
kafka_configure.read('kafka_config.ini')

class GlobalCounter:
    def __init__(self):
        self.current_global_counter_val = configure['Counter_Variable']['processed_row_count']
        self.records_in_a_batch = configure['Batch_Upload_to_DB']['records_in_a_batch']

    def fetch_records(self,car_data):
        return car_data.iloc[list(range(int(self.current_global_counter_val),int(self.current_global_counter_val)+int(self.records_in_a_batch)))]
    
    def set_global_counter_val(self):
        self.current_global_counter_val = str(int(self.current_global_counter_val)+int(self.records_in_a_batch))
        configure['Counter_Variable']['processed_row_count'] = str(int(self.current_global_counter_val))
    
    def write_config_data(self):
        with open('config.ini', 'w') as configfile:
            configure.write(configfile)
    
    def reset_global_counter_val(self):
        self.current_global_counter_val = str(0)
        configure['Counter_Variable']['processed_row_count'] = self.current_global_counter_val
        #need to figure this out
        configure['Batch_Upload_to_DB']['records_in_a_batch'] = self.records_in_a_batch
        self.write_config_data()

class kafka_utils:
    def sasl_conf():
        sasl_conf = {'sasl.mechanism': kafka_configure['Security_Params']['SSL_MACHENISM'],
                    'bootstrap.servers':kafka_configure['Kafka_Cluster_Config']['BOOTSTRAP_SERVER'],
                    'security.protocol': kafka_configure['Security_Params']['SECURITY_PROTOCOL'],
                    'sasl.username': kafka_configure['Kafka_Cluster_Config']['API_KEY'],
                    'sasl.password': kafka_configure['Kafka_Cluster_Config']['API_SECRET_KEY']
                    }
        return sasl_conf

    def schema_config():
        return {'url':kafka_configure['Schema_Registry']['ENDPOINT_SCHEMA_URL'],
        'basic.auth.user.info':f"{kafka_configure['Schema_Registry']['SCHEMA_REGISTRY_API_KEY']}:{kafka_configure['Schema_Registry']['SCHEMA_REGISTRY_API_SECRET']}"

        }
    
class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"

