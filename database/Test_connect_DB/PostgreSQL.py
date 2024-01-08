import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
import time
from sqlalchemy import create_engine
import io
conn = psycopg2.connect(database="test_dbt_dagster",
                        user="postgres",
                        host='localhost',
                        password="docker",
                        port=5432)

cursor = conn.cursor()

# csv = pd.read_csv("D:/Desktop/Test/Test_dbt_dag_postgres/data/raw_data.csv")
# df = pd.DataFrame(csv)
start_time = time.time()
#engine = create_engine("postgresql://postgres:docker@localhost:5432/test_dbt_dagster")
#sql_insert = "INSERT INTO raw_table (First_name,Last_name,Age,Sex) VALUES (%s, %s, %s, %s)"
#df_source_list = df.values.tolist()
#df.to_sql('raw_table',engine,schema=None, if_exists='append',method='multi')
#execute_batch(cursor,sql_insert,df_source_list)
#cursor.executemany(sql_insert,df_source_list)


with open('D:/Desktop/Test/Test_dbt_dag_postgres/data/raw_data.csv', 'r') as file:
    # Skip header if needed
    next(file)
    # Create a file-like object
    file_obj = io.StringIO(file.read())
    cursor.copy_from(file_obj, 'raw_table', sep=',')
    conn.commit()

# conn.commit()
# # for i in df_source_list:
# #     cursor.execute(sql_insert,sql_insert,df_source_list)

# # # # Close the cursor and connection
# cursor.close()
# conn.close()


print("--- %s seconds ---" % (time.time() - start_time))