import base64
import json
import requests
import time
import datetime
import logging
import os

import pandas as pd

import boto3

# from awsglue.context import GlueContext
# from awsglue.utils import getResolvedOptions
# from awsglue.job import Job

from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession

from pyspark.sql.functions import current_date, date_format

# # parâmetros de execução do Glue (se houver)
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# configura o SparkContext e o GlueContext
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# inicializando o job Glue
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)
spark = SparkSession.builder.getOrCreate()
# spark =ss.sparkContext

s3 = boto3.resource('s3')
bucket_name  = 'Fiap-ibov/'
s3_path = 'raw/'


log_dir = '/content/log'
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# configura o Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Função para gerar o valor base64 da URL
def generate_encoded_param(page_number):
    try:
        # Não vamos mais passar page_size, index e segment
        page_size = 20  # Tamanho de cada página
        index = "IBOV"  # Índice IBOV

        # Dicionário com os dados fixos
        data = {
            "language": "pt-br",
            "pageNumber": page_number,
            "pageSize": page_size,  # Tamanho da página
            "index": index,         # Índice
        }

        # Codificando os dados para base64
        json_data = json.dumps(data)
        encoded_data = base64.b64encode(json_data.encode()).decode('utf-8')
        logger.info(f"Parâmetro gerado para a página {page_number}: {encoded_data[:50]}...")  # Log do valor gerado
        return encoded_data
    except Exception as e:
        logger.error(f"Erro ao gerar o parâmetro base64 para a página {page_number}: {e}")
        raise

# Função principal para buscar e salvar os dados
def fetch_and_save_data():
    all_data = []

    headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'referer': 'https://sistemaswebb3-listados.b3.com.br/'
}


    try:
        # Fazer a primeira requisição para obter o total de páginas
        encoded_param = generate_encoded_param(1)  # Passando o número da página diretamente
        url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encoded_param}"

        logger.info(f"Requisitando dados para a página 1...")
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logger.error(f"Erro ao buscar dados: {response.status_code}")
            return

        # Obter o total de páginas da resposta JSON
        json_response = response.json()
        total_pages = json_response.get('page', {}).get('totalPages', 0)

        if total_pages == 0:
            logger.warning("Nenhuma página disponível na resposta.")
            return

        logger.info(f"Total de páginas: {total_pages}")

        # Loop por todas as páginas
        for page_number in range(1, total_pages + 1):
            logger.info(f"Buscando a página {page_number} de {total_pages}...")
            encoded_param = generate_encoded_param(page_number)  # Passando o número da página diretamente
            url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encoded_param}"

            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    json_response = response.json()
                    page_data = json_response.get('results', [])
                    if page_data:
                        all_data.extend(page_data)  # Adiciona dados à lista principal
                        logger.info(f"Página {page_number}: {len(page_data)} registros encontrados.")
                    else:
                        logger.warning(f"Página {page_number} está vazia ou não possui dados.")
                else:
                    logger.error(f"Falha ao buscar a página {page_number}: {response.status_code}")
            except Exception as e:
                logger.error(f"Erro ao buscar dados da página {page_number}: {e}")

            time.sleep(1)  # Atraso de 1 segundo entre requisições

        # Converter os dados em DataFrame
        df = pd.DataFrame(all_data)
        print(df.head())
        logger.info(f"Total de registros encontrados: {len(all_data)}")


        df.to_csv(f's3a://{bucket_name}/{s3_path}/dados_ibov_{datetime.date.today()}.csv', index=False)

        # Retornar DataFrame (opcional, dependendo do uso posterior)
        return df
    except Exception as e:
        logger.error(f"Erro na execução do processo: {e}")
        return None

# Executar a função principal
df = fetch_and_save_data()

if df is None:
    logger.error("Failed to fetch data. Exiting.")
    exit(1)

schema = StructType([
    StructField("segment", StringType(), True),
    StructField("cod", StringType(), True),
    StructField("asset", StringType(), True),
    StructField("type", StringType(), True),
    StructField("part", StringType(), True),
    StructField("theoretQtty", LongType(), True),
    StructField("updateDate", StringType(), True),
])
spark_df = spark.createDataFrame(df, schema=schema)

# coluna de data de processamento no formato "yyyyMMdd"
df = spark_df.withColumn("dataproc", date_format(current_date(), "yyyyMMdd").cast("int"))

spark_df.write.mode("overwrite").parquet(f's3a://{bucket_name}/{s3_path}/dados_ibov_{datetime.date.today()}.parquet')
# spark_df.write.parquet(f's3a://{bucket_name}/{s3_path}')
# spark_df.write.parquet("/content/log/")
logger.info("Processo concluído com sucesso.")
spark.stop()
