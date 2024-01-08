import psycopg2
import paramiko


if( __name__ == '__main__'):
    private_key = paramiko.RSAKey.from_private_key_file('C:/Users/Nuttanich/.ssh/id_rsa')
    ssh = paramiko.SSHClient()
    ssh.load_host_keys('C:/Users/Nuttanich/.ssh/known_hosts')
    # ssh.set_missing_host_key_policy(paramiko.RejectPolicy())
    ssh.connect('13.76.193.149',username='stream', pkey=private_key)
    
    # ssh_tunnel = ssh.get_transport().open_channel('direct-tcpip', ('13.76.193.149', 5433), ('localhost', 5432))

    # เชื่อมต่อกับฐานข้อมูล PostgreSQL
    connection = psycopg2.connect(
        host='127.0.0.1',  # หรือชื่อเซิร์ฟเวอร์
        port='5433',
        dbname='TMD_PORTAL_IOT_PLATFORM_DB',
        user='postgres',
        password='9597feebfd6a19f6075809d0df01df9c',
        # unix_socket=ssh_tunnel
    )

    # สร้าง Cursor object สำหรับการประมวลผลคำสั่ง SQL
    cursor = connection.cursor()

    query = f"select entity_id , s.name ,str_v \
            from attributes join stations s on entity_id = id \
            where  attribute_key = 'station_status' "
    cursor.execute(query)
    data = cursor.fetchall()
    print(data)

    # ปิด Cursor
    cursor.close()

    # ปิดการเชื่อมต่อฐานข้อมูล
    connection.close()

    # ssh_tunnel.close()
    ssh.close()






              
          

