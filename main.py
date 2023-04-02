import socket

# Задаем адрес сервера
SERVER_ADDRESS = ('', 3000)

# Настраиваем сокет
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(SERVER_ADDRESS)
server_socket.listen(10)
print('server is running, please, press ctrl+c to stop')

# Слушаем запросы
while True:
    connection, address = server_socket.accept()
    print("new connection from {address}".format(address=address))

    data = connection.recv(3)
    print(str(data))

    connection.send(bytes('Hello from server!', encoding='UTF-8'))

    connection.close()