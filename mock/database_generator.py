import os
from dataclasses import dataclass
from random import choice
import models
import sqlite3

neighborhood_create = '''
            CREATE TABLE neighborhood (
                id TEXT,
                name TEXT
            )
        '''

user_create = '''
            CREATE TABLE user (
                id TEXT,
                name TEXT,
                last_name TEXT,
                birth_date TEXT,
                neighborhood_id TEXT
            )
        '''

store_create = '''
            CREATE TABLE store (
                id TEXT,
                cnpj TEXT,
                name TEXT,
                neighborhood_id TEXT,
                weight_fee REAL
            )
        '''

product_create = '''
            CREATE TABLE product (
                id TEXT,
                name TEXT,
                price REAL, 
                weight_fee REAL
            )
        '''


stock_create = '''
            CREATE TABLE stock (
                id_product TEXT,
                id_store TEXT,
                quantity INT,
                PRIMARY KEY (id_product, id_store)             
            )
        '''

neighborhood_insert = '''
        INSERT INTO neighborhood (id, name)
        VALUES (?, ?)
    '''

user_insert = '''
        INSERT INTO user (id, name, last_name, birth_date, neighborhood_id)
        VALUES (?, ?, ?, ?, ?)
    '''

store_insert = '''
        INSERT INTO store (id, cnpj, name, neighborhood_id, weight_fee)  
        VALUES (?, ?, ?, ?, ?)
    '''

product_insert = '''
        INSERT INTO product (id, name, price, weight_fee)
        VALUES (?, ?, ?, ?)
    '''

stock_insert = '''
        INSERT INTO stock (id_product, id_store, quantity)  
        VALUES (?, ?, ?)
    '''

create = {'neighborhood': neighborhood_create, 'user': user_create, 'product': product_create, 'store': store_create, 'stock': stock_create}
insert = {'neighborhood': neighborhood_insert, 'user': user_insert, 'product': product_insert, 'store': store_insert, 'stock': stock_insert}

@dataclass
class DatabaseGeneratorParams:
    num_neighborhoods: int
    num_users: int
    num_products: int
    num_stores: int
    qtd_stock_initial: int

class DatabaseGenerator:
    params: DatabaseGeneratorParams
    cycle: int
    silent: bool = True

    def __init__(self, params: DatabaseGeneratorParams, silent: bool = True):
        self.cycle = 0
        self.params = params
        self.silent = silent
        self.neighborhoods = []
        self.neighborhood_ids = []
        self.users = []
        self.user_ids = [] #Apagar
        self.stores = []
        self.store_ids = []
        self.products = []
        self.product_ids = []
        self.stock = {}

        self.folder_name = "mock/mock_files"
        self.subfolder_sqlite3 = "sqlite3"
        self.sqlite3_file_names = ["neighborhood.sqlite3", "users.sqlite3", "store.sqlite3", "products.sqlite3", "stock.sqlite3", ]
        self.sqlite3_complete_path = [f"{self.folder_name}/{self.subfolder_sqlite3}/{file_name}" for file_name in self.sqlite3_file_names]

        # If the folder exists, delete its contents
        if os.path.exists(self.folder_name):
            self.remove_folder_contents(self.folder_name)
        else:
            os.makedirs(self.folder_name)

        # Create inside folder_name or delete other folder if they exist for the other subfolders
        sqlite3_folder = f"{self.folder_name}/{self.subfolder_sqlite3}"

        for folder in [sqlite3_folder]:
            if os.path.exists(folder):
                self.remove_folder_contents(folder)
            else:
                os.makedirs(folder)

        # Generate users at the start of the simulation
        for _ in range(self.params.num_neighborhoods):
            self.__generate_neighborhood()

        # Generate users at the start of the simulation
        for _ in range(self.params.num_users):
            self.__generate_user()

        # Generate products at the start of the simulation
        for _ in range(self.params.num_stores):
            self.__generate_store()

        # Generate products at the start of the simulation
        for _ in range(self.params.num_products):
            self.__generate_product()

        # Generate stock for the products
        for store in self.store_ids:
            for product in self.product_ids:
                self.__generate_stock(product, store, self.params.qtd_stock_initial)

    
    def run(self):
         # Report users, products and stocks created, creating the new data bases
        content_neighborhood = [[neighborhood.id, neighborhood.name] for neighborhood in self.neighborhoods]
        self.__connect('neighborhood', 0, content_neighborhood)

        content_user = [[user.id, user.name, user.last_name, user.birth_date, user.neighborhood_id] for user in self.users]
        self.__connect('user', 1, content_user)

        content_store = [[store.id, store.cnpj, store.name, store.neighborhood_id, store.weight_fee] for store in self.stores]
        self.__connect('store', 2, content_store)

        content_product = [[product.id, product.name, product.price, product.weight] for product in self.products]
        self.__connect('product', 3, content_product)

        content_stock = [[store_product_id[1], store_product_id[0], quantity] for store_product_id, quantity in self.stock.items()]
        self.__connect('stock', 4, content_stock)


    def __connect(self, data_type, index, content):
        db_path = self.sqlite3_complete_path[index]
        db_exists = os.path.exists(db_path)
        
        # Conectar ao banco de dados
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Criar tabela se o banco de dados não existir
        if not db_exists:
            cursor.execute(create[data_type])
            conn.commit()

        for item in content:
            cursor.execute(insert[data_type], item)

        conn.commit()
        conn.close()

    def remove_folder_contents(self, folder_path):
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)     
    
    def __generate_neighborhood(self):
        neighborhood = models.generate_neighborhood()
        self.neighborhood_ids.append(neighborhood.id)
        self.neighborhoods.append(neighborhood)

    def __generate_user(self):
        neighborhood_id = choice(self.neighborhood_ids)
        user = models.generate_user(neighborhood_id)
        self.user_ids.append(user.id) # Apagar
        self.users.append(user)

    def __generate_store(self):
        neighborhood_id = choice(self.neighborhood_ids)
        store = models.generate_store(neighborhood_id)
        self.store_ids.append(store.id)
        self.stores.append(store)

    def __generate_product(self):
        product = models.generate_product()
        self.product_ids.append(product.id)
        self.products.append(product)      

    def __generate_stock(self, product_id, store_id, quantity):
        stock_product = models.generate_stock(product_id, store_id, quantity)
        key = (stock_product.id_store, stock_product.id_product)
        self.stock[key] = stock_product.quantity  