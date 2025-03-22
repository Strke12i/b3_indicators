from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd, cloudscraper, logging, os, base64
from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

BASE_URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
    delay=1.5,  # Evita bloqueios de rate limiting
)

def _encode_payload(page: int, page_size: int = 100, language: str = "pt-br") -> str:
    """
    Codifica o payload para a requisição em Base64.
    """
    payload = f'{{"language":"{language}","pageNumber":{page},"pageSize":{page_size}}}'
    base64_bytes = base64.b64encode(payload.encode('ascii'))
    return str(base64_bytes, 'ascii')

def _fetch_page(page: int) -> dict:
    """
    Busca os dados de uma única página.
    """
    encoded_payload = _encode_payload(page)
    url = BASE_URL + encoded_payload
    response = scraper.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro na requisição. Status code: {response.status_code}")
    
    return response.json()

def _fetch_all_companies():
    """
    Busca todas as empresas listadas na B3 e retorna um DataFrame com os resultados.
    """
    # Busca a primeira página para determinar o número total de páginas
    try:
        first_page_data = _fetch_page(1)
        total_pages = first_page_data['page']['totalPages']
        all_results = first_page_data['results']
        # Itera pelas páginas restantes
        for page in range(2, total_pages + 1):
            page_data = _fetch_page(page)
            all_results.extend(page_data['results'])

        df = pd.DataFrame(all_results)
        # Salva o df em um arquivo CSV na pasta data
        data_dir = os.path.join(BASE_DIR, 'data')
        os.makedirs(data_dir, exist_ok=True)
        df.to_csv(os.path.join(data_dir, 'companies.csv'), index=False)
        
        logger.info("Empresas salvas em companies.csv")
    except Exception as e:
        logger.error(f"Erro ao buscar empresas: {str(e)}")
        raise


default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    'fetch_companies',
    default_args=default_args,
    description='Pipeline para buscar e processar dados de empresas listadas na B3',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    concurrency=8,  # Controlar a concorrência
    tags=['b3', 'reports', 'financial_data'],
)

fetch_companies_task = PythonOperator(
    task_id='fetch_companies',
    python_callable=_fetch_all_companies,
    dag=dag,
)


fetch_companies_task



