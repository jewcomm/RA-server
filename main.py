import socket
import sqlite3
import time

# Задаем адрес сервера
SERVER_ADDRESS = ('', 3000)

PACKET_LENGTH = 232

IMEI_BEGIN = 0
IMEI_END = 8

COUNT_MEAS_BEGIN = IMEI_END
COUNT_MEAS_END = 16

MEASURMENT_DATA_LENGTH = 36

MEASURMENT_BEGIN = 20
MEASURMENT_LENGTH = 24

MEASURMENT_ID_BEGIN = 16
MEASURMENT_ID_END = 20

def createrDB(conn : sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS defaults(
        default_id INTEGER PRIMARY KEY AUTOINCREMENT,
        imei INT,
        meas_id INT,
        meas_def BLOB
        );
    """)

    conn.commit()

    cur.execute("""CREATE TABLE IF NOT EXISTS measurement(
        default_id INT,
        timestamp REAL,
        result INT,
        FOREIGN KEY(default_id) REFERENCES defaults(default_id));
    """)

    conn.commit()
    cur.close()

def parser(data : bytes, conn : sqlite3.Connection):
    imei = int.from_bytes(data[IMEI_BEGIN:IMEI_END], byteorder="little", signed=False)

    count = int.from_bytes(data[COUNT_MEAS_BEGIN:COUNT_MEAS_END], byteorder="little", signed=False)

    print(str(data))

    print("IMEI = " + str(imei))
    print("Count = " + str(count))
    for meas in range(count):
        offset = meas * MEASURMENT_DATA_LENGTH
        measId = int.from_bytes(data[MEASURMENT_ID_BEGIN + offset:MEASURMENT_ID_END + offset], byteorder="little")
        measResult = data[MEASURMENT_BEGIN + offset:MEASURMENT_BEGIN + MEASURMENT_LENGTH + offset]
        print("Measurment ID: " + str(measId) + " = " + str(measResult))
        ts = time.time()
        result = True
        if(found_in_db(imei=imei, meas_id=measId, conn=conn)):
            ret = insert_into_measurement(imei=imei, meas_id=measId, meas_res=measResult, timestamp=ts, conn=conn)
            if(result):
                result = ret
        else:
            insert_into_defaults(imei=imei, meas_id=measId, meas_def=measResult, conn=conn)
    print( result )  

def found_in_db(imei : int, meas_id : int, conn : sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""SELECT * FROM defaults where imei=? and meas_id=?""", (imei, meas_id))
    check = cur.fetchall()
    cur.close()
    return check != []

def insert_into_defaults(imei : int, meas_id : int, meas_def : int, conn : sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""INSERT INTO defaults(imei, meas_id, meas_def) VALUES(?, ?, ?)""", (imei, meas_id, meas_def))
    conn.commit()
    cur.close()

def insert_into_measurement(imei : int, meas_id : int, meas_res : int, timestamp : float, conn : sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""SELECT default_id, meas_def FROM defaults where imei=? and meas_id=?""", (imei, meas_id))
    res = cur.fetchall()
    print(res)
    print(res[0][0])
    print(res[0][1])
    check = (res[0][1] == meas_res)
    print(check)
    cur = conn.cursor()
    cur.execute("""INSERT INTO measurement VALUES(?, ?, ?)""", (res[0][0], timestamp, check))
    conn.commit()
    cur.close()
    return check

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

        parser(data, conn)

        connection.send(bytes('Hello from server!', encoding='UTF-8'))

        connection.close()