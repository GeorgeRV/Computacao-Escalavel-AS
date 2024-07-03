# pipeline_project/budget_simulator/budget_calculator.py
from common.celery import app, logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame
import sqlite3
import redis
import json
import time
import multiprocessing

# Conectar ao Redis
r = redis.Redis(host='redis', port=6379, db=0)

lock = multiprocessing.Lock()

spark = SparkSession.builder \
    .appName("Calculando Menor Distância entre Bairros") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .getOrCreate()

def get_user_data(user_id):
    key = f"user:{user_id}"
    user_data = r.hgetall(key)
    # Convertendo os dados para strings
    user_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in user_data.items()}
    return user_data

def get_store_data(store_id):
    key = f"store:{store_id}"
    store_data = r.hgetall(key)
    # Convertendo os dados para strings
    store_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in store_data.items()}
    return store_data

def get_product_data(product_id):
    key = f"product:{product_id}"
    user_data = r.hgetall(key)
    # Convertendo os dados para strings
    user_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in user_data.items()}
    return user_data

def get_neighborhood_data(neighborhood_id):
    key = f"neighborhood:{neighborhood_id}"
    neighborhood_data = r.hgetall(key)
    neighborhood_data = {k.decode(): v.decode() for k, v in neighborhood_data.items()}
    return neighborhood_data

def get_stock_data(product_id, store_id):
    key = f"stock:{store_id}:{product_id}"
    stock_data = r.hgetall(key)
    return stock_data

def get_neighbors_data(neighborhood1_id, neighborhood2_id):
    key = f"neighbors:{neighborhood1_id}:{neighborhood2_id}"
    neighbors_data = r.hgetall(key)
    neighbors_data = {k.decode(): v.decode() for k, v in neighbors_data.items()}
    return neighbors_data

def neighborhood_access():
    vertices_data = []
    for key in r.keys("neighborhood:*"):
        vertex_data = r.hgetall(key)
        vertex_data = {k.decode(): v.decode() for k, v in vertex_data.items()}
        vertices_data.append(vertex_data)
    
    # Recuperar dados de arestas
    edges_data = []
    for key in r.keys("neighbors:*"):
        decoded_key = key.decode().split(":")
        neighborhood1_id = decoded_key[1]
        neighborhood2_id = decoded_key[2]
        nei = get_neighbors_data(neighborhood1_id,neighborhood2_id)
        distance = nei["distance"]
        edge_data = {
            "src": neighborhood1_id,
            "dst": neighborhood2_id,
            "distance": int(distance)
        }
        edges_data.append(edge_data)
        inverse_edge_data = {       # Adicionar as arestas inversas
        "src": neighborhood2_id,    # para um grafo não direcionado
        "dst": neighborhood1_id,
        "distance": int(distance)
        }
        edges_data.append(inverse_edge_data)

    # Criar DataFrame de vértices
    vertices_df = spark.createDataFrame(vertices_data)
    
    # Renomear coluna para 'id'
    vertices_df = vertices_df.withColumnRenamed('id', 'id')
    
    # Criar DataFrame de arestas
    edges_df = spark.createDataFrame(edges_data)
    
    # Criar GraphFrame
    grafo = GraphFrame(vertices_df, edges_df)

    return grafo

grafo = neighborhood_access()

@app.task
def calculate_budget(data):
    # Implementação da função de cálculo do orçamento
    start_time = time.time()
    budget = json.loads(data)

    logger.info(f"Recebido orçamento: {budget}")

    user  = get_user_data(budget["id_user"])
    store = get_store_data(budget["id_store"])
    product = get_product_data(budget["id_product"])

    quantity = int(budget["quantity"])
    product_price = product["price"]
    base_price = float(product_price) * quantity

    user_neighborhood_id = user["neighborhood_id"]
    store_neighborhood_id = store["neighborhood_id"]
    
    start_time_2 = time.time()
    menor_distancia = grafo.shortestPaths(landmarks=[user_neighborhood_id, store_neighborhood_id])
    end_time_2 = time.time()

    user_distances_row = menor_distancia.filter(col("id") == user_neighborhood_id).select("distances").collect()
    user_distances = user_distances_row[0]["distances"]
    least_distance = user_distances.get(store_neighborhood_id, float("inf"))

    product_weight = product["weight"]
    store_weight_fee = store["weight_fee"]
    delivery_fee = float(product_weight) * quantity * float(store_weight_fee) * float(least_distance)

    budget_price = base_price + delivery_fee

    end_time = time.time()

    # Calcula o tempo decorrido
    elapsed_time = end_time - start_time
    #logger.info(f"Tempo decorrido: {elapsed_time:.6f} segundos")
    elapsed_time_2 = end_time_2 - start_time_2
    logger.info(f"Tempo decorrido no menor caminho: {elapsed_time_2:.6f} segundos")
    elapsed_time_3 = start_time_2 - start_time + end_time - end_time_2
    #logger.info(f"Tempo decorrido sem o menor caminho: {(elapsed_time_3):.6f} segundos")
    #logger.info(f'A menor distância entre {user_neighborhood_id} e {store_neighborhood_id} é: {least_distance}')
    #logger.info(f"Orçamento = {budget_price}")

    budget["price"] = budget_price
    budget["state"] = "Pendente"
    
    new_budget = json.dumps(budget)

    return new_budget

@app.task
def recive_order(data):

    budget = json.loads(data)
    logger.info(f"Recebido orçamento: {budget}")

    if(budget["state"] == "Aprovado"):
    
        folder_name = "mock_files/sqlite3/stock.sqlite3"

        order_store_id = budget["id_store"]
        order_product_id = budget["id_product"]
        order_quantity = budget["quantity"]

        conn = sqlite3.connect(folder_name)
        cursor = conn.cursor()

        try:
            # Adquire o mutex
            with lock:
                # Verifica o estoque do produto na loja específica
                cursor.execute("SELECT quantity FROM stock WHERE id_product = ? AND id_store = ?", (order_product_id, order_store_id))
                result = cursor.fetchone()

                stock_quantity = result[0]
                if stock_quantity >= order_quantity:
                    # Atualiza o estoque
                    new_quantity = stock_quantity - order_quantity
                    cursor.execute("UPDATE stock SET quantity = ? WHERE id_product = ? AND id_store = ?", (new_quantity, order_product_id, order_store_id))
                    conn.commit()
                    logger.info(f"Estoque atualizado. Nova quantidade do produto {order_product_id} na loja {order_store_id}: {new_quantity}")
                else:
                    print(f"Estoque insuficiente para o produto {order_product_id} na loja {order_store_id}.")
                    budget["state"] = "Cancelado"

        except Exception as e:
            print(f"Erro ao acessar o banco de dados: {e}")
        finally:
            conn.close()

    #print(ids)
    print(data)