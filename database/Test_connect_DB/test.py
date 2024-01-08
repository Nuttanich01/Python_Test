import psycopg2
import pandas as pd
import psycopg2.extras
import numpy as np

conn = psycopg2.connect(database='tmd_transform',
user='test',
password='mysecretpassword',
host='localhost',
port=54325)

cursor = conn.cursor()

df = pd.read_csv("D:\Desktop\Test\Test_code\Test_connect_DB\cbi301.csv")
print(df.info())
# Convert DataFrame to a list of tuples with 'int' type
df_list = df.values.tolist()
# print(df_list)
query_target = """INSERT INTO tmd_transform.tmd_staging.cbi301 (Timestamp ,uniqueIdentifier ,firmwareVersion ,configVersion ,stationType ,sensorCode ,battery ,batt_voltage , 
   battery_current ,pv_current ,pv_voltage ,load_current,isCharging ,isDoorOpen ,
   temp_device ,humi_device ,rssi ,locLat ,locLong,altitude,isSleep,air_pressure,tempurature,
   humidity,rain_gauge ,wind_speed ,wind_direction ,"pm_2.5" ,pm_10 ,soil_moisture ,rain_5min ,
   rain_15min ,rain_30min ,rain_1hour ,rain_3hour ,rain_6hour ,rain_12hour ,rain_24hour ,rain_48hour,
   rain_72hour ,wind_speed_avg ,wind_direction_avg ,deviceCreatedAt,sequence) VALUES %s"""

psycopg2.extras.execute_values(cursor, query_target, df_list , page_size=10000)
conn.commit()
cursor.close()
conn.close()