import socket
import sqlite3

# Задаем адрес сервера
SERVER_ADDRESS = ('', 3000)

PACKET_LENGTH = 240

IMEI_BEGIN = 0
IMEI_END = 16

COUNT_MEAS_BEGIN = 16
COUNT_MEAS_END = 24

MEASURMENT_DATA_LENGTH = 36

MEASURMENT_BEGIN = 28
MEASURMENT_LENGTH = 32

MEASURMENT_ID_BEGIN = 24
MEASURMENT_ID_END = 28

def createrDB(conn : sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS devices(
        deviceid INT PRIMARY KEY,
        imei TEXT NOT NULL); 
    """)

    conn.commit()

    cur.execute("""CREATE TABLE IF NOT EXISTS references(
        deviceid INT,
        measid INT,
        measref BLOB);
    """)

    conn.commit()

    cur.execute("""CREATE TABLE IF NOT EXISTS measurement(
        deviceid INT,
        timestamp INT,
        result BLOB);
    """)

    conn.commit()

def parser(data : bytes):
    imei = data[IMEI_BEGIN:IMEI_END]

    count = int.from_bytes(data[COUNT_MEAS_BEGIN:COUNT_MEAS_END], byteorder="little", signed=False)

    print(str(data))

    print("IMEI = " + str(imei))
    print("Count = " + str(count))
    for meas in range(count):
        offset = meas * MEASURMENT_DATA_LENGTH
        measId = int.from_bytes(data[MEASURMENT_ID_BEGIN + offset:MEASURMENT_ID_END + offset], byteorder="little")
        measResult = data[MEASURMENT_BEGIN + offset:MEASURMENT_BEGIN + MEASURMENT_LENGTH + offset]
        print("Measurment ID: " + str(measId) + " = " + str(measResult))

if __name__ == "__main__":
    conn = sqlite3.connect('meas.sqlite')
    
    createrDB(conn)

    # Настраиваем сокет
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(SERVER_ADDRESS)
    server_socket.listen(10)
    print('server is running, please, press ctrl+c to stop')

    # Слушаем запросы
    while True:
        connection, address = server_socket.accept()
        print("new connection from {address}".format(address=address))

        data = connection.recv(PACKET_LENGTH)

        parser(data)

        connection.send(bytes('Hello from server!', encoding='UTF-8'))

        connection.close()