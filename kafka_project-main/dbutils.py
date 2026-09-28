import pymysql as pm
import pandas as pd
from configparser import ConfigParser
from queries import create_table_query,use_database_query
from sqlalchemy import create_engine

db_conf = ConfigParser()
db_conf.read('db_config.ini')

class Dbutils:
    def __init__(self):
        self.endpoint = db_conf['Db_config']['conn_endpoint']
        self.username = db_conf['Db_config']['Username']
        self.password = db_conf['Db_config']['Password']
        self.port = db_conf['Db_config']['Port']
        self.database = db_conf['Db_config']['database']
        self.table_name = db_conf['Db_config']['table_name']
        self.engine=create_engine(f"mysql+pymysql://{self.username}:{self.password}@{self.endpoint}/{self.database}")

    def db_connect(self):
        try:
            self.cnx = pm.connect(host=self.endpoint,user=self.username,\
                              password=self.password,port=int(self.port),database=self.database)
            
            self.cursor = self.cnx.cursor()
        except Exception as e:
            print("Exception {} Occured while trying to establish Connection.".format(e))
    
    def create_table(self):
        query = create_table_query.format(table_name=self.table_name)
        print("query is : \n{}".format(query))
        self.cursor.execute(query)

    def use_database(self,db_name):
        query = use_database_query.format(db_name)
        print("use database query is : \n{}".format(query))
        self.cursor.execute(query)

    def insert_records(self,df):
        df.to_sql(name=self.table_name, con=self.engine, if_exists="append", index=False)
    
    def run_query(self,query):
        return pd.read_sql_query(query,self.engine)

    def close_connection(self):
        self.cnx.close()