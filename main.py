import asyncio
import io
import re
import aiohttp
import redis
from socket import socket, AF_INET, SOCK_STREAM
from store import Retailer

MAX_BYTES = 1024
BASE_TARGET_URL = 'https://jsonplaceholder.typicode.com/photos'
EXPIRATION_TIME = 3600


class Server:

    def __init__(self, redis_instance: redis.Redis = redis.Redis(db=4), retail: Retailer = Retailer()):
        self._server_socket = None
        self._set_server()
        self._redis_instance = redis_instance
        self._retailer = retail

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
        data_cached = self._redis_instance.get(key)
        if data_cached:
            return data_cached
        async with aiohttp.ClientSession() as session:
            response_from_service = await session.request(method='GET', url=key)
            data = await response_from_service.read()
            self._redis_instance.setex(key, EXPIRATION_TIME, data)
        return data

    async def _get_targeted_data(self, target: str) -> bytes:
        order_from_supplier = re.search(r'/retailer/order/[0-9]+', target)
        if order_from_supplier:
            positions_count = order_from_supplier.group().split('/')[3]
            self._retailer.order_latest_from_supplier(int(positions_count))
            return b'Goods delivered'
        if target == '/retailer/info':
            data = b''
            for item_id, item_info in self._retailer.get_all_info():
                data += item_id
                data += b'\r\n'
                for k, v in item_info.items():
                    data += k + b':' + v + b'\r\n'
                data += b'\r\n'
            return data
        if target == '/retailer/order/':
            self._retailer.order_latest_from_supplier(1)
        key = ''
        if target == '/photos':
            key = BASE_TARGET_URL
        single_photo_request = re.search(r'^/photos/[0-9]+$', target)
        if single_photo_request:
            photo_id = single_photo_request.group().split('/')[2]
            key = BASE_TARGET_URL + '/' + photo_id
        data = await self._get_or_set_cache(key)
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
                sock.close()
                break
            target = self._get_target(request=io.BytesIO(request))
            data = await self._get_targeted_data(target)
            response = self._form_valid_response(data)
            sock.send(response)

    def show_cache_keys(self) -> None:
        print(self._redis_instance.keys())

    def flush_cache(self) -> None:
        self._redis_instance.flushall()


if __name__ == '__main__':
    server = Server()
    try:
        asyncio.run(server.serve())
    except KeyboardInterrupt:
        info_to_be_flushed = input('Flush all data from redis?')
        if info_to_be_flushed.lower() in ['y', 'yes', 'ok']:
            server.show_cache_keys()
            server.flush_cache()

"""
Bulk writing without enabling redis: 286 ms 1.02 MB, which is 3x faster than with node.js axios in similar conditions.
Bulk writing with redis caching: 4 ms 1.02 MB, which is 5x faster than with node.js axios in similar conditions.

Single photo request handling before caching: 387 ms for 281 B of message
After caching: 2-4 ms.
"""
