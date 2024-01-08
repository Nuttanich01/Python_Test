import paramiko
import psycopg2

# ตัวอย่างการเชื่อมต่อเซิร์ฟเวอร์ผ่าน SSH ด้วย paramiko
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect('hostname_or_ip', username='your_username', password='your_password')

# ตัวอย่างการเชื่อมต่อฐานข้อมูล PostgreSQL ผ่าน SSH ด้วย psycopg2
# สร้าง SSH Tunnel
ssh_tunnel = client.get_transport().open_channel('direct-tcpip', ('database_hostname', 5432), ('localhost', 5432))

# เชื่อมต่อฐานข้อมูล PostgreSQL ผ่าน SSH Tunnel
connection = psycopg2.connect(host='localhost', port=5432, user='db_username', password='db_password', database='db_name')

# ทำสิ่งที่คุณต้องการกับฐานข้อมูล
# เช่น ดึงข้อมูลจากตาราง
try:
    with connection.cursor() as cursor:
        sql = 'SELECT * FROM table_name'
        cursor.execute(sql)
        result = cursor.fetchall()
        for row in result:
            print(row)

finally:
    # ปิดการเชื่อมต่อฐานข้อมูล
    connection.close()

# หลังจากเสร็จสิ้นการทำงาน ออกจากการเชื่อมต่อ SSH
client.close()
