import random
import redis
from faker import Faker


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
    def produce(cls, amount: int):
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
