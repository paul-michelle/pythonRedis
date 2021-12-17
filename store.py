import random
import redis
from faker import Faker
from typing import List, Tuple, Dict, Optional

fake = Faker()


class Conveyor:

    def __init__(self):
        self.hash = f'item:{random.getrandbits(32)}'
        self.description = {
            'designer': fake.name(),
            'date': fake.date(),
            'price': fake.random_int(1, 100),
            'quantity': fake.random_int(20, 50),
            'purchased': 0
        }

    @classmethod
    def produce(cls, amount: int) -> List['Conveyor']:
        produced = []
        for i in range(amount):
            produced.append(cls())
        return produced


class Supplier:

    def __init__(self):
        self._redis_instance = redis.Redis(db=4)

    def deliver(self, amount: int):
        with self._redis_instance.pipeline() as pipe:
            for item in Conveyor.produce(amount):
                pipe.hmset(item.hash, item.description)
            pipe.execute()


class OutOfStockError(Exception):
    pass


class NotEnoughInStock(Exception):
    """ e.g.:
    __main__.NotEnoughInStock: Quantity available of item:1810785242 is b'27'
    """
    pass


class Retailer:

    def __init__(self, supplier: Supplier = Supplier()):
        self._redis_instance = redis.Redis(db=4)
        self._supplier = supplier

    def order_latest_from_supplier(self, positions_count: int = 0):
        self._supplier.deliver(positions_count)

    def show_goods_available(self) -> List[bytes]:
        """Get list of ids, e.g.:
        [b'item:2833270290', b'item:162468497', b'item:3066974616']
        """
        return self._redis_instance.keys()

    def get_item_info(self, item_id: str) -> Dict[bytes, bytes]:
        return self._redis_instance.hgetall(item_id)

    def get_all_info(self) -> List[Tuple[bytes, Dict[bytes, bytes]]]:
        """ Receive a list of tuples with info.
        (b'item:960156607',
          {b'date': b'2017-07-01',
           b'designer': b'Gabriela Carter',
           b'price': b'87',
           b'purchased': b'0',
           b'quantity': b'29'})
        """
        return [(item_id, self.get_item_info(item_id.decode('utf-8')))
                for item_id in self.show_goods_available()]

    def sell(self, item_id: str, quantity_to_sell: int) -> Optional[str]:
        """ e.g.:
        'Sold 27 of item:1810785242. Revenue: 2322'
        """
        with self._redis_instance.pipeline() as pipe:
            error_count = 0
            while True:
                try:
                    pipe.watch(item_id)
                    quantity_in_stock: bytes = self._redis_instance.hget(item_id, 'quantity')
                    if quantity_in_stock < str(quantity_to_sell).encode('utf-8'):
                        raise NotEnoughInStock(f'Quantity available of {item_id} is {quantity_in_stock}')
                    if quantity_in_stock > b'0':
                        pipe.multi()
                        pipe.hincrby(item_id, 'quantity', -quantity_to_sell)
                        pipe.hincrby(item_id, 'purchased', quantity_to_sell)
                        pipe.execute()
                        revenue = quantity_to_sell * int(self._redis_instance.hget(item_id, 'price').decode('utf-8'))
                        return f'Sold {quantity_to_sell} of {item_id}. Revenue: {revenue}'
                    pipe.unwatch()
                    raise OutOfStockError(f'{item_id} is out of stock. Contact the supplier')
                except redis.WatchError:
                    error_count += 1
                    print(f'Tried to sell {item_id}, failed {error_count} time(s).')

    def refresh(self) -> str:
        """
        retailer = Retailer()
        retailer.refresh()
        'Everything up-to-date' | 'Info refreshed. Number of entries deleted: 2'
        """
        try:
            with self._redis_instance.pipeline() as pipe:
                entries_deleted: int = 0
                pipe.multi()
                for item_id, _ in self.get_all_info():
                    if self._redis_instance.hget(item_id, 'quantity') <= b'0':
                        self._redis_instance.delete(item_id)
                        entries_deleted += 1
                pipe.execute()
                if entries_deleted:
                    return f'Info refreshed. Number of entries deleted: {entries_deleted}'
                return f'Everything up-to-date'
        except redis.RedisError as e:
            return f'Redis: Exception occurred. {e}'
