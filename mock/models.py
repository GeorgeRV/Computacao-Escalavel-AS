from dataclasses import dataclass
from faker import Faker
from functools import wraps

@dataclass
class Neighborhood:
    id: str
    name: str

@dataclass
class User:
    id: str
    name: str
    last_name: str
    birth_date: str
    neighborhood_id: str

@dataclass
class Store:
    id: str
    cnpj: str
    name: str
    neighborhood_id: str
    weight_fee: float

@dataclass
class Product:
    id: str
    name: str
    price: float
    weight: float

@dataclass
class Stock:
    id_product: str
    id_store: str
    quantity: int

@dataclass
class Neighbors:
    neighborhood1_id: str
    neighborhood2_id: str
    distance: int


# Singleton decorator
def singleton(cls):
    @wraps(cls)
    def wrapper(*args, **kwargs):
        if not hasattr(cls, "_instance"):
            cls._instance = cls(*args, **kwargs)
        return cls._instance
    return wrapper

@singleton
class FakerSingleton:
    def __init__(self):
        self._faker = None

    def get_faker(self):
        if not self._faker:
            self._faker = Faker('pt_BR')
            self._faker.seed_instance(4321)
        return self._faker


def generate_neighborhood():
    """Generates a Neighborhood dataclass instance with unique data."""
    faker = FakerSingleton().get_faker()
    return Neighborhood(
        id=faker.unique.numerify(text="0##"),
        name=faker.neighborhood()
    )

def generate_user(neighborhood_id):
    """Generates a User dataclass instance with unique data."""
    faker = FakerSingleton().get_faker()
    return User(
        id=faker.unique.numerify(text="1####"),
        name=faker.first_name(),
        last_name=faker.last_name(),
        birth_date=faker.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
        neighborhood_id = neighborhood_id
    )

def generate_store(neighborhood_id):
    """Generates a Store dataclass instance with unique data."""
    faker = FakerSingleton().get_faker()
    return Store(
        id=faker.unique.numerify(text="0##"),
        cnpj=faker.cnpj(),
        name=faker.company(),
        neighborhood_id = neighborhood_id,
        weight_fee= round(faker.pyfloat(left_digits=1, right_digits=1, positive=True, min_value=0.5, max_value=2),1)
    )

def generate_product():
    """Generates a Product dataclass instance with unique data."""
    faker = FakerSingleton().get_faker()

    return Product(
        id=faker.unique.numerify(text="2######"),
        name=faker.word(),
        price=faker.random_int(min=1, max=1000),
        weight=faker.pyfloat(left_digits=2, right_digits=1, positive=True, min_value=0.5, max_value=50)
    )

def generate_stock(product_id: str, store_id: str, quantity: int):
    """Generates a Stock dataclass instance."""
    return Stock(
        id_product=product_id,
        id_store=store_id,
        quantity=quantity,
    )

def generate_neighbors(neighborhood1_id, neighborhood2_id, distance):
    """Generates a Neighbors dataclass instance with unique data."""
    return Neighbors(
        neighborhood1_id = neighborhood1_id,
        neighborhood2_id = neighborhood2_id,
        distance = distance
    )