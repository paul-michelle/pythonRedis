import asyncio
import io
import aiohttp
import redis
from socket import socket, AF_INET, SOCK_STREAM

MAX_BYTES = 1024
BASE_TARGET_URL = 'https://jsonplaceholder.typicode.com/photos'
EXPIRATION_TIME = 3600


class Server:

    def __init__(self):
        self._server_socket = None
        self._set_server()
        self._redis = redis.Redis()

    def _set_server(self):
        self._server_socket = socket(AF_INET, SOCK_STREAM)
        self._server_socket.bind(('', 9999))
        self._server_socket.listen(1)

    def _get_client_socket(self):
        client_socket, _ = self._server_socket.accept()
        return client_socket

    @staticmethod
    def _get_target(request: io.BytesIO):
        header = request.readline()
        _, target, _ = str(header, 'iso-8859-1').rstrip().split()
        return target

    async def _get_or_set_cache(self, key: str) -> bytes:
        data_cached = self._redis.get(key)
        if data_cached:
            return data_cached
        async with aiohttp.ClientSession() as session:
            response_from_service = await session.request(method='GET', url=key)
            data = await response_from_service.read()
            self._redis.setex(key, EXPIRATION_TIME, data)
        return data

    async def _get_targeted_data(self, target: str) -> bytes:
        data = b''
        if target == '/photos':
            data = await self._get_or_set_cache(key=BASE_TARGET_URL)
        return data

    @staticmethod
    def _form_valid_response(data: bytes):
        status_line = b'HTTP/1.1 200 OK\r\n'
        headers = [('Content-Type', 'application/json; charset=utf-8'), ('Content-Length', len(data))]
        headers_lines = b''
        for key, value in headers:
            headers_lines += f'{key}:{value}\r\n'.encode('iso-8859-1')
        return status_line + headers_lines + b'\r\n' + data

    async def serve(self):
        sock = self._get_client_socket()
        while True:
            request = sock.recv(MAX_BYTES)
            if not request:
                break
            target = self._get_target(request=io.BytesIO(request))
            data = await self._get_targeted_data(target)
            response = self._form_valid_response(data)
            sock.send(response)


if __name__ == '__main__':
    server = Server()
    try:
        asyncio.run(server.serve())
    except KeyboardInterrupt:
        pass

"""
Bulk writing without enabling redis: 286 ms 1.02 MB, which is 3x faster than with node.js axios in similar conditions.
Bulk writing with redis caching: 4 ms 1.02 MB, which is 5x faster than with node.js axios in similar conditions.
"""