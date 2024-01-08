
import psycopg2
import psycopg2.extras

conn = psycopg2.connect(database='tmd_transform_two', 
   user='test', 
   password='mysecretpassword', 
   host='localhost', 
   port=54326)
conn_cursor = conn.cursor()
sql_query = """insert into temp_telemetry_data \
    (entity_id,entity_type,"key",created_at,server_created_at,str_v,dbl_v,bool_v) \
    select entity_id,entity_type,"key",created_at,server_created_at,str_v,dbl_v,bool_v \
    from telemetry_data \
    WHERE created_at >= CURRENT_DATE - INTERVAL '1 day' \
    AND created_at < CURRENT_DATE + INTERVAL '1 day';"""
conn_cursor.execute()
conn.commit()
conn_cursor.close()
conn.close()
