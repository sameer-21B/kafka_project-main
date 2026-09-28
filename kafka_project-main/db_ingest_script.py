import configuration
import dbutils
import importlib
importlib.reload(configuration)
importlib.reload(dbutils)
from configuration import GlobalCounter as gc
from dbutils import Dbutils as db
import pandas as pd
import time,sys
from datetime import datetime
import logging
import sys,os

logger=logging.getLogger(__name__)
logfile_nm=(str(__file__)[:-3]+'_'+str(datetime.now().strftime('%Y%m%d-%H%M%S'))).split('/')[-1]
print("Logfile_nm is :",logfile_nm)

logging.basicConfig(filename=f'./Logs/{logfile_nm}.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filemode='w')

logger.setLevel(logging.INFO)


def main_function(a=0,b=0):
    global logger,logfile_nm

    logger.info(f"Name of Log File : {logfile_nm}")

    print("Accessing Raw Data")
    car_data=pd.read_csv('s3://input-data-car-sales/cardekho_dataset.csv',index_col=0)
    car_data['Idx']=range(1,len(car_data)+1)
    car_data=car_data[['Idx']+list(car_data.columns)[:-1]]
    logger.info("Dataframe has been created")


    gc_obj=gc()
    db_obj=db()

    print("Trying to connect to DB")

    try:
        db_obj.db_connect()
        logger.info("DB Connection was Successful.")
    except Exception as e:
        logger.error(f"Exception {e} occured while Connecting to Database")
    
    print("DB Connection has been Established")

    db_obj.cursor.execute('show tables;')
    output=db_obj.cursor.fetchall()
    create_tbl=True
    for i in range(len(list(output))):
        if list(output)[i][0]==db_obj.table_name:
            logger.info(f"{db_obj.table_name} Table is already created.")
            create_tbl = False

    if create_tbl == True : 
        logger.info(f"{db_obj.table_name} Table was Created.")
        db_obj.create_table()

    if len(sys.argv)>1:
        records=int(list(sys.argv)[1])
    else:
        records=len(car_data)
    logger.info(f"Number of Records to be Processed is {records}.")

    logger.info("Resetting Global Counter value to Zero.")
    gc_obj.reset_global_counter_val()

    while (int(gc_obj.current_global_counter_val)+int(gc_obj.records_in_a_batch)<=len(car_data) \
        and int(gc_obj.current_global_counter_val)+int(gc_obj.records_in_a_batch)<=records):
        logger.info(f"Current Value of Global Counter is {gc_obj.current_global_counter_val}")
        batch_of_data = gc_obj.fetch_records(car_data)
        logger.info(f"Incrementing Global counter value by {gc_obj.records_in_a_batch}")
        gc_obj.set_global_counter_val()
        logger.info(f"Commiting Config data changes.")
        gc_obj.write_config_data()
        db_obj.insert_records(batch_of_data)
        logger.info(f"Records from index {int(gc_obj.current_global_counter_val)-int(gc_obj.records_in_a_batch)} to {int(gc_obj.current_global_counter_val)} were inserted into table.")
        time.sleep(5)

    logger.info("Resetting Global Counter Value to Zero.")
    gc_obj.reset_global_counter_val()

if __name__=="__main__":
    try:
        main_function()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)