import sqlite3
from dataclasses import dataclass
from random import choice, randint
from datetime import datetime, timedelta
from budget_simulator.budget_calculator import calculate_budget, recive_order
from json import dumps, loads
import multiprocessing
import time
import random
from faker import Faker

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

        self.fake = Faker()

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

        num_processes = multiprocessing.cpu_count()
        processes = []

        for _ in range(num_processes):
            process = multiprocessing.Process(target=self.__generate_delivery)
            processes.append(process)
            process.start()

        # Aguardar até que todos os processos terminem
        for process in processes:
            process.join()

    def __generate_delivery(self):
        # Implementação da função de geração de entrega

        result = self.__send_budget()
        
        result = result.get(timeout=60)

        print(f"O orçamento recebido é {result}")

        self.__send_order(result)

    def __send_budget(self):
        time.sleep(random.randint(5, 15))

        delivery_data = self.__create_order()
        delivery_data_json = dumps(delivery_data)

        print(delivery_data_json)
        print(delivery_data)
        
        # Chamar a função de cálculo do orçamento de forma assíncrona
        result = calculate_budget.delay(delivery_data_json)

        return result
    
    def __send_order(self, result):
        time.sleep(random.randint(15, 25))  # Tempo simulado para aprovar ou não o orçamento
        
        budget_result = loads(result)
        
        if random.random() < 0.5:
            budget_result["state"] = "Aprovado"
        else:
            budget_result["state"] = "Reprovado"

        budget_result_json = dumps(budget_result)

        # Chamar a função de pedido de forma assíncrona
        recive_order.delay(budget_result_json)


    def __create_order(self):
        user = choice(self.users_ids)
        store = choice(self.stores_ids)
        product = choice(self.products_ids)
        quantity = randint(self.params.min_qtd_order, self.params.max_qtd_order)
        order_id = str(self.fake.uuid4())  # Gerar um ID único
        order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Obter a data atual formatada
        
        return {
            "id": order_id,
            "date": order_date,
            "id_user": user,
            "id_store": store,
            "id_product": product,
            "quantity": quantity,
            "state": "Criado",
            "price": ""
        }

  


if __name__ == "__main__":
    params = DeliveryParams(
            num_deliveries=20,
            min_qtd_order=1,
            max_qtd_order=10  
        )

    delivery = Delivery(params)
    delivery.run()