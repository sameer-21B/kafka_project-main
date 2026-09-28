use_database_query = \
'''
use {db_name};'''

create_table_query =  \
'''create table {table_name} (
	Idx integer,
	Car_name varchar(50),
	Brand varchar(50),
	Model varchar(50),
	Vehicle_age integer,
	Km_driven integer,
	Seller_type varchar(20),
	Fuel_type varchar(20),
	Transmission_type varchar(20),
	Mileage float,
	Engine integer,
	Max_power float,
	Seats integer,
	Selling_price integer,
	Record_ld_dt Timestamp Not Null default CURRENT_TIMESTAMP
);'''