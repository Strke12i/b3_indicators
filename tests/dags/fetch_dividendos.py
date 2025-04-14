from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
import pandas as pd, cloudscraper, logging, os, base64
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
BASE_URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedCashDividends/'
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
    delay=1.5,  # Evita bloqueios de rate limiting
)

def _encode_payload(page: int, trading_name: str, page_size: int = 100, language: str = "pt-br") -> str:
    """
    Codifica o payload para a requisição em Base64.
    """
    payload = f'{{"language":"{language}","pageNumber":{page},"pageSize":{page_size},"tradingName":"{trading_name}"}}'
    base64_bytes = base64.b64encode(payload.encode('ascii'))
    return str(base64_bytes, 'ascii')

def _fetch_data(trading_name: str):
    try:
        payload = _encode_payload(1, trading_name)
        url = BASE_URL + payload
        
        request = scraper.get(url)
        if request.status_code != 200:
            raise Exception(f"Erro na requisição. Status code: {request.status_code}")
        response = request.json()
        if 'results' not in response:
            raise Exception(f"Erro na requisição. Mensagem: {response}")
        
        df = pd.DataFrame(response['results'])
        if 'page' not in response:
            logger.info("Apenas uma página de resultados")
            return df
        total_pages = response['page']['totalPages']
        for page in range(2, total_pages + 1):
            payload = _encode_payload(page, trading_name)
            url = BASE_URL + payload
            request = scraper.get(url)
            if request.status_code != 200:
                raise Exception(f"Erro na requisição. Status code: {request.status_code}")
            response = request.json()
            df = pd.concat([df, pd.DataFrame(response['results'])], ignore_index=True)
        if df.empty:
            raise Exception("Nenhum resultado encontrado")
        return df
    except Exception as e:
        logger.error(f"Erro ao buscar dividendos: {str(e)}")
        raise


def _fetch_dividendos(**kwargs):
    try:
        conf = kwargs.get('dag_run').conf or {}
        trading_name = conf.get('trading_name')
        if not trading_name:
            raise Exception("Nome da empresa não informado!")
        
        # Busca os dividendos e ordena por data de aprovação
        df = _fetch_data(trading_name)
        df = df.sort_values('dateApproval', ascending=False)
        
        engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        Session = sessionmaker(bind=engine)
        session = Session()

        metadata = MetaData(bind=engine)
        dividendos = Table('dividendos', metadata, autoload_with=engine)
        print(dividendos.columns.keys())
        
        # Encontra todos os dividendos da empresa
        query = session.query(dividendos).filter(dividendos.c.nome_empresa == trading_name)
        datas_dividendos = [dividendo.data_aprovacao for dividendo in query.all()]
        
        # Buscar a data de aprovação mais recente
        if datas_dividendos:
            ultima_data = max(datas_dividendos)
            df = df[df['dateApproval'] > ultima_data]
        
        df['dateApproval'] = pd.to_datetime(df['dateApproval'], errors='coerce', format='%d/%m/%Y')
        df['lastDatePriorEx'] = pd.to_datetime(df['lastDatePriorEx'], errors='coerce', format='%d/%m/%Y')
        df['dateClosingPricePriorExDate'] = pd.to_datetime(df['dateClosingPricePriorExDate'], errors='coerce', format='%d/%m/%Y')
        
        df['valueCash'] = df['valueCash'].str.replace(',', '.').astype(float)
        df['quotedPerShares'] = df['quotedPerShares'].str.replace(',', '.').astype(float)
        df['corporateActionPrice'] = df['corporateActionPrice'].str.replace(',', '.').astype(float)
        df['closingPricePriorExDate'] = df['closingPricePriorExDate'].str.replace(',', '.').astype(float)
        
        trading_name = trading_name.upper()
    except Exception as e:
        logger.error(f"Erro ao buscar dividendos: {str(e)}")
        raise
        # Insere os dividendos no banco de dados
    for _, row in df.iterrows():
        try:
            session.execute(dividendos.insert().values(
                nome_empresa=trading_name,
                data_aprovacao=row['dateApproval'],
                tipo_ativo=row['typeStock'],
                valor_provento=row['valueCash'],
                proventos_por_unidade=row['ratio'],
                tipo_provento=row['corporateAction'],
                ultimo_dia_com=row['lastDatePriorEx'],
                data_ultimo_preco_com=row['dateClosingPricePriorExDate'],
                ultimo_preco_com=row['closingPricePriorExDate'],
                preco_por_unidade=row['quotedPerShares'],
                provento_preco_percentual=row['corporateActionPrice'],
            ))
        except Exception as e:
            logger.error(f"Erro ao inserir dividendo: {str(e)}")
            session.rollback()
    session.commit()
    session.close()
    

        #Ordenar por data de aprovação

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
    'b3_fetch_dividendos',
    default_args=default_args,
    description='Pipeline otimizado para buscar e processar dividendos da B3',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    concurrency=8,  # Controlar a concorrência
    tags=['b3', 'reports', 'financial_data'],
)

t1 = PythonOperator(
    task_id='fetch_dividendos',
    python_callable=_fetch_dividendos,
    provide_context=True,
    dag=dag,
)

t1