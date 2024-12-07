import base64
import json
import requests
import pandas as pd
import time
import datetime
import logging

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'./log/fetch_ibov_log_{datetime.date.today()}.log'),  # Salva logs no arquivo
        logging.StreamHandler()  #exibe logs no console
    ]
)

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
        logging.info(f"Parâmetro gerado para a página {page_number}: {encoded_data[:50]}...")  # Log do valor gerado
        return encoded_data
    except Exception as e:
        logging.error(f"Erro ao gerar o parâmetro base64 para a página {page_number}: {e}")
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

        logging.info(f"Requisitando dados para a página 1...")
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            logging.error(f"Erro ao buscar dados: {response.status_code}")
            return

        # Obter o total de páginas da resposta JSON
        json_response = response.json()
        total_pages = json_response.get('page', {}).get('totalPages', 0)

        if total_pages == 0:
            logging.warning("Nenhuma página disponível na resposta.")
            return

        logging.info(f"Total de páginas: {total_pages}")

        # Loop por todas as páginas
        for page_number in range(1, total_pages + 1):
            logging.info(f"Buscando a página {page_number} de {total_pages}...")
            encoded_param = generate_encoded_param(page_number)  # Passando o número da página diretamente
            url = f"https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetPortfolioDay/{encoded_param}"

            try:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    json_response = response.json()
                    page_data = json_response.get('results', [])
                    if page_data:
                        all_data.extend(page_data)  # Adiciona dados à lista principal
                        logging.info(f"Página {page_number}: {len(page_data)} registros encontrados.")
                    else:
                        logging.warning(f"Página {page_number} está vazia ou não possui dados.")
                else:
                    logging.error(f"Falha ao buscar a página {page_number}: {response.status_code}")
            except Exception as e:
                logging.error(f"Erro ao buscar dados da página {page_number}: {e}")

            time.sleep(1)  # Atraso de 1 segundo entre requisições

        # Converter os dados em DataFrame
        df = pd.DataFrame(all_data)
        logging.info(f"Total de registros encontrados: {len(all_data)}")

        df.to_csv(f'./data/dados_ibov_{datetime.date.today()}.csv', index=False)

        # Retornar DataFrame (opcional, dependendo do uso posterior)
        return df
    except Exception as e:
        logging.error(f"Erro na execução do processo: {e}")
        return None

# Executar a função principal
df = fetch_and_save_data()

