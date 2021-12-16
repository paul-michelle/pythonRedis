from socket import socket, AF_INET, SOCK_STREAM

MAX_BYTES = 1024


class Server:

    def __init__(self):
        self._server_socket = None
        self._set_server()

    def _set_server(self):
        self._server_socket = socket(AF_INET, SOCK_STREAM)
        self._server_socket.bind(('', 9999))
        self._server_socket.listen(1)

    def _get_client_socket(self):
        client_socket, _ = self._server_socket.accept()
        return client_socket

    def serve(self):
        sock = self._get_client_socket()
        while True:
            data = sock.recv(MAX_BYTES)
            if not data:
                break
            sock.send(b'Received: ' + data)


if __name__ == '__main__':
    server = Server()
    server.serve()
