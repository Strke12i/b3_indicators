from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd, cloudscraper, logging, os, base64
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import insert


logger = logging.getLogger(__name__)

URL_BASE = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/"
scrapper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
    delay=1.5,  # Evita bloqueios de rate limiting
)
URL_IMAGE = "https://br.tradingview.com/symbols/BMFBOVESPA-"

def _fetch_companies_from_db():
    try:
        # Carrega o DataFrame de empresas
        df = pd.read_sql_table('empresa', 'postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        df = df[df['ticker'].isna() | df['data_ipo'].isna() | df['url_imagem'].isna()]
        
        logger.info("Empresas carregadas do banco de dados")
        return df
    except Exception as e:
        logger.error(f"Erro ao buscar empresas: {str(e)}")
        raise Exception(f"Erro ao buscar empresas: {str(e)}")

def _encode_payload(company_id: int) -> str:
    """
    Codifica o payload para a requisição em Base64.
    """
    payload = f'{{"codeCVM":"{company_id}","language":"pt-br"}}'
    base64_bytes = base64.b64encode(payload.encode('ascii'))
    return str(base64_bytes, 'ascii')

def _fetch_company_information(company_id: int) -> dict:
    """
    Busca as informações de uma empresa a partir do seu código CVM.
    """
    encoded_payload = _encode_payload(company_id)
    url = URL_BASE + encoded_payload
    response = scrapper.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro na requisição. Status code: {response.status_code}")
    
    return response.json()

def _fetch_url_image(code: str):
    try:
        code = code.upper()
        url = URL_IMAGE + code
        
        response = scrapper.get(url)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        img = soup.find('img', class_='logo-PsAlMQQF xlarge-PsAlMQQF small-xoKMfU7r wrapper-TJ9ObuLF skeleton-PsAlMQQF')
        if img is None:
            logger.error(f"Imagem da empresa {code} não encontrada")
            return None
        
        logger.info(f"Imagem da empresa {code} encontrada")
        logger.info(img['src'])
        return img['src']
    except Exception as e:
        logger.error(f"Erro ao buscar imagem da empresa {code}: {str(e)}")
        return None
    

def _extract_company_information(df: pd.DataFrame):
    """
    Extrai as informações de cada empresa do DataFrame.
    """
    tickers = {}
    for idx, row in df.iterrows():
        company_id = row['id_empresa']
        try:
            company_info = _fetch_company_information(company_id)
            logger.info(f"Informações da empresa {company_id} extraídas")
            logger.info(company_info)
            if df.loc[idx, 'ticker'] is None:
                df.loc[idx, 'ticker'] = company_info.get('code', None)

            if company_info.get('otherCodes'):
                tickers[company_id] = company_info.get('otherCodes')

            if df.loc[idx, 'data_ipo'] is pd.NaT or df.loc[idx, 'data_ipo'] is None:
                date_quotation = company_info.get('dateQuotation', None)
                if date_quotation is not None:
                    logger.info(f"Data de IPO da empresa {company_id}: {date_quotation}")
                    df.loc[idx, 'data_ipo'] = datetime.strptime(date_quotation, "%d/%m/%Y")

            if df.loc[idx, 'ticker'] is not None and df.loc[idx, 'url_imagem'] is None:
                df.loc[idx, 'url_imagem'] = None #_fetch_url_image(df.loc[idx, 'ticker'])

        except Exception as e:
            logger.error(f"Erro ao buscar informações da empresa {company_id}: {str(e)}")
            continue

    return {
        'df': df, 
        'tickers': tickers
    }

def _transform_tickers(data: dict):
    tickers = data.get('tickers', {})
    
    rows = [(key, item_dict['code'],item_dict['isin'])
        for key, value_list in tickers.items()
        for item_dict in value_list]

    # Criar o DataFrame a partir da lista de tuplas
    df = pd.DataFrame(rows, columns=['id_empresa', 'ticker', 'isin'])
    
    return df

def _save_companies_to_db(data: dict):
    df = data.get('df', pd.DataFrame())
    logger.info("Salvando empresas no banco de dados")
    logger.info(df.head())
    print(df[['id_empresa', 'data_ipo']])
    try:
        # Cria a engine e a sessão
        engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define a tabela
        metadata = MetaData(bind=engine)
        companies_table = Table('empresa', metadata, autoload_with=engine)

        df['data_ipo'] = pd.to_datetime(df['data_ipo'], errors='coerce')
        df['data_ipo'] = df['data_ipo'].replace({pd.NaT: None})

        print(df[['id_empresa', 'data_ipo']])
        # Itera sobre o DataFrame e atualiza os registros
        for idx, row in df.iterrows():
            stmt = (
                companies_table.update()
                .where(companies_table.c.id_empresa == row['id_empresa'])
                .values(
                    ticker=row['ticker'],
                    data_ipo=row['data_ipo'],
                    url_imagem=row['url_imagem']
                )
            )
            session.execute(stmt)

        # Commit das alterações
        session.commit()
        logger.info("Empresas atualizadas no banco de dados")
    except Exception as e:
        logger.error(f"Erro ao salvar empresas no banco de dados: {str(e)}")
        raise
    finally:
        session.close()

def _save_tickers_to_db(df: pd.DataFrame):
    from sqlalchemy.dialects.postgresql import insert
    try:
        engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        Session = sessionmaker(bind=engine)
        session = Session()
        metadata = MetaData(bind=engine)
        tickers_table = Table('ticker', metadata, autoload_with=engine)
        for idx, row in df.iterrows():
            try:
                stmt = insert(tickers_table).values(
                    id_empresa=row['id_empresa'],
                    ticker=row['ticker'],
                    codigo_isin=row['isin']
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['codigo_isin'],
                    set_={
                        'ticker': row['ticker'],
                        'id_empresa': row['id_empresa'],
                        'codigo_isin': row['isin']
                    }
                )
                session.execute(stmt)
            except Exception as e:
                logger.error(f"Erro ao inserir ou atualizar ticker no banco de dados: {str(e)}")
        session.commit()
        logger.info("Tickers successfully saved to the database")
    except Exception as ex:
        logger.error(f"Erro ao salvar tickers no banco de dados: {str(ex)}")
        session.rollback()
        raise
    finally:
        session.close()
    

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
    'atualiza_dados_de_empresas',
    default_args=default_args,
    description='Busca informações de empresas listadas na B3',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    concurrency=8,  # Controlar a concorrência
    tags=['b3', 'reports', 'financial_data'],
)

t1 = PythonOperator(
    task_id='fetch_companies_from_db',
    python_callable=_fetch_companies_from_db,
    dag=dag,
)
t2 = PythonOperator(
    task_id='extract_company_information',
    python_callable=_extract_company_information,
    dag=dag,
    op_args=[t1.output],
)

t3 = PythonOperator(
    task_id='transform_tickers',
    python_callable=_transform_tickers,
    dag=dag,
    op_args=[t2.output],
)

t4 = PythonOperator(
    task_id='save_companies_to_db',
    python_callable=_save_companies_to_db,
    dag=dag,
    op_args=[t2.output],
)

t5 = PythonOperator(
    task_id='save_tickers_to_db',
    python_callable=_save_tickers_to_db,
    dag=dag,
    op_args=[t3.output],
)

t1 >> t2 >> t3 >> t4 >> t5

