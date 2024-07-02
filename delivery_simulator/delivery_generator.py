import sqlite3
from dataclasses import dataclass
from random import choice, randint
from datetime import datetime, timedelta
from budget_simulator.budget_calculator import calculate_budget
from json import dumps


@dataclass
class DeliveryParams:
    num_deliveries: int
    min_qtd_order: int
    max_qtd_order: int

class Delivery:
    params: DeliveryParams

    def __init__(self, params: DeliveryParams):
        self.params = params
        self.users_ids = []
        self.stores_ids = []
        self.products_ids = []

        self.folder_name = "mock_files"
        self.subfolder_sqlite3 = "sqlite3"
        self.sqlite3_names = ["user", "store", "product"]
        self.sqlite3_complete_path = [f"{self.folder_name}/{self.subfolder_sqlite3}/{file_name}.sqlite3" for file_name in self.sqlite3_names]

        self.ids_list = []

        for index, path in enumerate(self.sqlite3_complete_path):
            conn = sqlite3.connect(path)
            cursor = conn.cursor()
            cursor.execute(f"SELECT id FROM {self.sqlite3_names[index]}")
            tables = cursor.fetchall()
            ids = [row[0] for row in tables]
            self.ids_list.append(ids)

        self.users_ids = self.ids_list[0]
        self.stores_ids = self.ids_list[1]
        self.products_ids = self.ids_list[2]

    def run(self):

        for _ in range(self.params.num_deliveries):
            self.generate_delivery()

        

    def generate_delivery(self):
        # Implementação da função de geração de entrega
        delivery_data = self.__create_order()

        delivery_data_json = dumps(delivery_data)
        
        # Chamar a função de cálculo do orçamento de forma assíncrona
        calculate_budget.delay(delivery_data_json)
    
        print(delivery_data_json)

        print(delivery_data)
        
        # Fazer algo com o resultado, se necessário
        #result = result.get(timeout=10)
        
        #return delivery_data

    def __create_order(self):
        user = choice(self.users_ids)
        store = choice(self.stores_ids)
        product = choice(self.products_ids)
        quantity = randint(self.params.min_qtd_order, self.params.max_qtd_order)
        return {"id_user": user, "id_store": store, "id_product": product, "quantity": quantity, "state": "Criado"}

  


if __name__ == "__main__":
    params = DeliveryParams(
            num_deliveries=2,
            min_qtd_order=1,
            max_qtd_order=10  
        )

    oi = Delivery(params)
    oi.run()