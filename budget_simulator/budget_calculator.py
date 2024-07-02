# pipeline_project/budget_simulator/budget_calculator.py
from common.celery import app, logger
from pyspark.sql import SparkSession
from graphframes import GraphFrame

spark = SparkSession.builder \
    .appName("Calculando Menor Distância entre Bairros") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.1-s_2.12") \
    .getOrCreate()

# Exemplo de dados de vértices e arestas
vertices = spark.createDataFrame([
    ("Bairro A",),
    ("Bairro B",),
    ("Bairro C",),
    ("Bairro D",),
    ("Bairro E",)
], ["id"])

arestas = spark.createDataFrame([
    ("Bairro A", "Bairro B"),
    ("Bairro B", "Bairro C"),
    ("Bairro B", "Bairro D"),
    ("Bairro C", "Bairro E"),
    ("Bairro D", "Bairro E")
], ["src", "dst"])

# Criando um GraphFrame
grafo = GraphFrame(vertices, arestas)

# Calculando a menor distância entre dois bairros específicos
menor_distancia = grafo.shortestPaths(landmarks=["Bairro A", "Bairro E"])

# Mostrando o resultado
menor_distancia.select("id", "distances").show(truncate=False)

# Encerrando a sessão Spark
spark.stop()

@app.task
def calculate_budget(data):
    # Implementação da função de cálculo do orçamento
    budget = data
    logger.info(f"Recebido orçamento: {budget}")
    print(budget)


if __name__ == "__main__":
        # Conectando ao banco de dados
    conn = sqlite3.connect('mock_files/sqlite3/neighborhood.sqlite3')
    cursor = conn.cursor()

    # Listar tabelas no banco de dados
    cursor.execute("SELECT id FROM neighborhood")
    tables = cursor.fetchall()

    # Extrair apenas os IDs da lista de tuplas
    ids = [row[0] for row in tables]

    print(ids)
