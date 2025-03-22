from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import TaskInstance
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
import base64, os, cloudscraper, requests, concurrent.futures
import logging, time, json
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker, Session
from CalculadoraIndicadoresFinanceiros import CalculadoraIndicadoresFinanceiros

# Configuração de logging
logger = logging.getLogger(__name__)

# Constantes
STRUCTURED_REPORTS_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListStructuredReports/"
CVM_BASE_URL = "https://www.rad.cvm.gov.br/ENET/"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMPANIES_CSV_PATH = os.path.join(BASE_DIR, 'data', 'companies.csv')
scraper = cloudscraper.create_scraper(
    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True},
    delay=1.5,  # Evita bloqueios de rate limiting
)

# Carregar o DataFrame de empresas apenas uma vez
df_companies = pd.read_csv(COMPANIES_CSV_PATH)

# Funções otimizadas para as tarefas
def get_company_info(**kwargs):
    conf = kwargs.get('dag_run').conf or {}

    empresa = conf.get('b3_empresa')
    ano = conf.get('b3_ano')

    if not empresa or not ano:
        raise ValueError("Parâmetros 'b3_empresa' e 'b3_ano' são obrigatórios.")

    company_info = df_companies[df_companies["issuingCompany"] == empresa]

    if company_info.empty:
        raise ValueError(f"Empresa '{empresa}' não encontrada.")

    code_cvm = company_info.iloc[0]['codeCVM']
    trading_name = company_info.iloc[0]['tradingName']

    # Retorna os valores como dicionário para o XCom
    return {"code_cvm": code_cvm, "empresa": empresa, "ano": int(ano), "trading_name": trading_name}


def get_structured_reports(code_cvm, year):
    try:
        payload = f'{{"codeCVM":{code_cvm},"language":"pt-br","status":true,"year":{year}}}'
        encoded_payload = base64.b64encode(payload.encode()).decode()
        url = STRUCTURED_REPORTS_URL + encoded_payload
        
        # Implementar retry com backoff exponencial
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = scraper.get(url, timeout=30)  # Adicionar timeout
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"Tentativa {attempt+1}/{max_retries}: Status code {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Backoff exponencial
            except Exception as e:
                logger.warning(f"Tentativa {attempt+1}/{max_retries}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        raise Exception(f"Erro na requisição após {max_retries} tentativas.")
    except Exception as e:
        logger.error(f"Erro ao obter relatórios estruturados para CVM {code_cvm}, ano {year}: {str(e)}")
        raise

def extract_url_search(data_json):
    try:
        data_json = json.loads(data_json)
        return data_json["dfp"][0]["urlSearch"]
    except Exception as e:
        logger.error(f"Erro ao extrair URL: {str(e)}")
        return None

def fetch_link_content(title, link):
    """Função auxiliar para buscar conteúdo de um link em paralelo"""
    try:
        response = scraper.get(link, timeout=30)
        return title, base64.b64encode(response.content).decode('utf-8')
    except Exception as e:
        logger.error(f"Erro ao extrair conteúdo do link {title}: {str(e)}")
        return title, None

def extract_links_from_select(url_report):
    if not url_report:
        logger.warning("URL do relatório vazia")
        return None
    
    try:
        response = scraper.get(url_report, timeout=30)
        soup = BeautifulSoup(response.content, "html.parser", parse_only=SoupStrainer(["select", "script"]))
        
        # Usar SoupStrainer para limitar a análise HTML
        select = soup.find("select", {"id": "cmbQuadro"})
        if not select:
            logger.warning("Select não encontrado na página")
            return None
            
        options = select.find_all("option")
        
        scripts = soup.find_all("script")
        if not scripts:
            logger.warning("Scripts não encontrados na página")
            return None
            
        script = scripts[-1]
        script_content = script.string if script.string else ""
        
        try:
            link_id = script_content.split("location=")[1].split("'")[1].split("Versao=")[1]
        except (IndexError, AttributeError):
            logger.warning("Não foi possível extrair link_id do script")
            return None
        
        links = {
            option.text: f"{CVM_BASE_URL}{option['value'].replace(' ', '%20')}{link_id}"
            for option in options
        }
        
        # Usar processamento paralelo para buscar links
        responses = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_title = {
                executor.submit(fetch_link_content, title, link): title
                for title, link in links.items()
            }
            
            for future in concurrent.futures.as_completed(future_to_title):
                title, content = future.result()
                if content:
                    responses[title] = content
        
        return responses
    except Exception as e:
        logger.error(f"Erro ao extrair links: {str(e)}")
        return None

def get_table_data(response):
    try:
        decoded_content = base64.b64decode(response).decode('utf-8')
        
        # Usar SoupStrainer para limitar a análise do HTML apenas à tabela necessária
        table_strainer = SoupStrainer("table", {"id": "ctl00_cphPopUp_tbDados"})
        soup = BeautifulSoup(decoded_content, "html.parser", parse_only=table_strainer)
        
        table = soup.find()
        if not table:
            raise Exception("Tabela não encontrada.")
        
        # Extração mais eficiente com list comprehension
        thead = table.find("thead")
        tbody = table.find("tbody")
        
        if thead and tbody:
            # Versão mais eficiente para tabelas com thead/tbody
            headers = [th.text.strip() for th in thead.find_all("th")]
            rows = [[td.text.strip() for td in tr.find_all("td")] for tr in tbody.find_all("tr")]
        else:
            # Fallback para o formato original
            rows = [[td.text.strip() for td in tr.find_all("td")] for tr in table.find_all("tr")]
            headers = rows[0]
            rows = rows[1:]
        
        return pd.DataFrame(rows, columns=headers)
    except Exception as e:
        logger.error(f"Erro ao obter dados da tabela: {str(e)}")
        raise

def reorganize_df(df):
    try:
        if 'Descrição' not in df.columns:
            logger.warning("Coluna 'Descrição' não encontrada no DataFrame")
            return df
            
        df.set_index('Descrição', inplace=True)
        
        # Remover colunas desnecessárias de forma segura
        if 'Conta' in df.columns:
            df.drop(columns=['Conta'], inplace=True)
        
        # Vetorizar conversão de strings para numérico
        numeric_cols = df.columns
        
        # Aplicar replace de forma otimizada
        for col in numeric_cols:
            if df[col].dtype == 'object':  # Verificar se a coluna é do tipo string
                df[col] = (df[col]
                          .str.replace('\.', '', regex=True)
                          .str.replace(',', '.', regex=True)
                          .apply(pd.to_numeric, errors='coerce'))
        
        df.fillna(0, inplace=True)
        
        # Remover duplicatas no índice de forma eficiente
        if df.index.duplicated().any():
            df.index = df.index + df.groupby(level=0).cumcount().astype(str).replace('0', '')

        return df
    except Exception as e:
        logger.error(f"Erro ao reorganizar DataFrame: {str(e)}")
        return df  # Retornar o DataFrame original em caso de erro

def fetch_data_from_data_type(url_report, links, company_name, data_type, year):
    if not url_report:
        raise ValueError(f"Não foi possível localizar relatórios para '{company_name}' no ano {year}.")
    
    if not links or data_type not in links:
        raise ValueError(f"Tipo de dado '{data_type}' não disponível para '{company_name}'.")

    response = links[data_type]
    df = get_table_data(response)
    df = reorganize_df(df)
    
    return df

def fetch_all_data(url_report, links, company_name, year):
    data_types = [
            "Balanço Patrimonial Ativo",
            "Balanço Patrimonial Passivo",
            "Demonstração do Resultado",
            #"Demonstração do Resultado Abrangente",
            "Demonstração do Fluxo de Caixa",
            #"Demonstração de Valor Adicionado",
    ]
    
    data = {}
    # Podemos paralelizar aqui, mas com cuidado para não sobrecarregar a API
    for data_type in data_types:
        try:
            df = fetch_data_from_data_type(url_report, links, company_name, data_type, year)
            data[data_type] = df
        except Exception as e:
            logger.error(f"Erro ao extrair dados de '{data_type}': {str(e)}")
            data[data_type] = None
    
    return data

def get_all_itrs_links(data_json):
    data_json = json.loads(data_json)
    itr_links = {}
    try:
        if not data_json or 'itr' not in data_json:
            logger.warning("Dados de ITR não encontrados no JSON")
            return itr_links
            
        itr = data_json['itr']
        for i in itr:
            try:
                date = i['dateTimeReference'].split('T')[0]
                itr_links[date] = i['urlSearch']
            except KeyError as e:
                logger.warning(f"Campo ausente nos dados ITR: {str(e)}")
                
        return itr_links
    except Exception as e:
        logger.error(f"Erro ao extrair links de ITR: {str(e)}")
        return itr_links

def transform_data(data: dict, year_rec) -> dict:
    if not data:
        return data
        
    for key in data.keys():
        if data[key] is None:
            continue
            
        if key in ['Balanço Patrimonial Ativo', 'Balanço Patrimonial Passivo']:
            for column in data[key].columns:
                try:
                    year = column.split('/')[-1]
                    new_column = f'01/01/{year}  a  {column}'
                    data[key].rename(columns={column: new_column}, inplace=True)
                except Exception as e:
                    logger.warning(f"Erro ao transformar coluna {column}: {str(e)}")

    return data

def verify_if_urls_already_searched(url_report, itr_links, code_cvm):
    if not url_report and not itr_links:
        return url_report, itr_links
    
    try:
        logger.info("Salvando resultados no banco de dados")
        engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define a tabela
        metadata = MetaData(bind=engine)
        urls = Table('urls_visitadas', metadata, autoload_with=engine)
        
        query = session.query(urls).filter_by(id_empresa=code_cvm)
        urls_visited = {url.url for url in query.all()} if query.all() else set()
        
        if url_report in urls_visited:
            url_report = None
        for date, link in itr_links.items():
            if link in urls_visited:
                itr_links[date] = None
        
        return url_report, itr_links
    except Exception as e:
        print(f"Erro ao verificar no banco: {e}")
        session.rollback()
        return url_report, itr_links

def save_urls_to_db(url_report, itr_links, code_cvm):
    try:
        logger.info("Salvando resultados no banco de dados")
        engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define a tabela
        metadata = MetaData(bind=engine)
        urls = Table('urls_visitadas', metadata, autoload_with=engine)
        
        if url_report:
            session.execute(urls.insert().values(
                id_empresa=code_cvm,
                url=url_report
            ))
            session.commit()
        
        for date, link in itr_links.items():
            if link:
                session.execute(urls.insert().values(
                    id_empresa=code_cvm,
                    url=link
                ))
                session.commit()
    except Exception as e:
        print(f"Erro ao salvar no banco: {e}")
        session.rollback()

def fetch_itr_data(url_report, dfp_links, itr_links, company_name, year, code_cvm):
    try:
        dfp_links = json.loads(dfp_links)
    except (TypeError, json.JSONDecodeError):
        dfp_links = {}

    try:
        itr_links = json.loads(itr_links)
    except (TypeError, json.JSONDecodeError):
        itr_links = {}

    if not itr_links:
        logger.warning(f"Não foram encontrados relatórios ITR para {company_name}")
        itr_links = {}  # Usar um dicionário vazio em vez de falhar
    

    url_report, itr_links = verify_if_urls_already_searched(url_report, itr_links, code_cvm)
    itr_data = {}
    
    # Processar ITRs trimestrais
    for date, link in itr_links.items():
        try:
            links_from_select = extract_links_from_select(link)
            if links_from_select:
                itr_data[date] = fetch_all_data(link, links_from_select, company_name, year)
            else:
                logger.warning(f"Não foi possível extrair links para data {date}")
        except Exception as e:
            logger.error(f"Erro ao processar ITR para data {date}: {str(e)}")
    
    # Processar DFP de final de ano
    try:
        if url_report or dfp_links:
            year = int(year)
            end_year = fetch_all_data(url_report, dfp_links, company_name, year + 1)
            date = end_year['Balanço Patrimonial Ativo'].columns[0]
            print(date)
            date = date.split('/')
            formated_date = f'{date[2]}-{date[1]}-{date[0]}'
            itr_data[formated_date] = end_year
        else:
            logger.warning("URL do relatório ou links DFP não encontrados")
    except Exception as e:
        logger.error(f"Erro ao processar DFP de final de ano: {str(e)}")
    
    # Transformar dados
    for key in list(itr_data.keys()):  # Usar list() para evitar erro de modificação durante iteração
        try:
            itr_data[key] = transform_data(itr_data[key], year)
        except Exception as e:
            logger.error(f"Erro ao transformar dados para data {key}: {str(e)}")

    return {
        'data': itr_data,
        'itr_links': itr_links,
        'url_report': url_report
    }

def convert_df_to_dict(data: dict) -> dict:
    result = {}
    for key in data.keys():
        result[key] = {}
        if data[key] is not None:
            for k in data[key].keys():
                if data[key][k] is not None:
                    try:
                        result[key][k] = data[key][k].to_dict()
                    except Exception as e:
                        logger.warning(f"Erro ao converter para dicionário {key}/{k}: {str(e)}")
                        result[key][k] = None
                else:
                    result[key][k] = None
        else:
            result[key] = None
    return result

def send_data_to_api(data, company_name, code_cvm):
    logging.info(f"Enviando dados para a empresa {company_name}")

    if not data:
        logger.warning(f"Não há dados para enviar para a empresa {company_name}")
        return
    
    if not data.get('data'):
        logger.warning(f"Não há dados para enviar para a empresa {company_name}")
    
    

    data_dict = convert_df_to_dict(data.get('data'))

    api_url = 'http://api:8000/insert-data'
        # Implementar retry com backoff exponencial
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.post(
                api_url,
                json={"company_name": company_name,
                      "code_cvm": code_cvm,
                      "data": data_dict},
                timeout=60  # Timeout mais longo para envio de dados
            )

            if response.status_code == 200:
                logger.info(f"Dados enviados com sucesso para a empresa {company_name}")
                save_urls_to_db(data.get('url_report', None), data.get('itr_links', None), code_cvm)
                break
            else:
                logger.warning(f"Tentativa {attempt+1}/{max_retries}: Status code {response.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Backoff exponencial
        except Exception as e:
            logger.warning(f"Tentativa {attempt+1}/{max_retries}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    else:
        logger.error(f"Falha ao enviar dados para a API após {max_retries} tentativas.")
        raise Exception("Erro ao enviar dados para a API.")

    logger.info("Todos os dados enviados com sucesso!")


def _save_results_to_db(df_resultado):
    try:
        logger.info("Salvando resultados no banco de dados")
        engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
        Session = sessionmaker(bind=engine)
        session = Session()

        # Define a tabela
        metadata = MetaData(bind=engine)
        indicators_table = Table('indicadores', metadata, autoload_with=engine)
    except Exception as e:
        print(f"Erro ao  no banco: {e}")
        session.rollback()
        # Salva os resultados no banco
    for _, row in df_resultado.iterrows():
        # Verifica se já existe um registro para a empresa e data_fim, caso haja, atualiza, senão, insere
        row['tempo_analisado'] = 12
        try:
            query = session.query(indicators_table).filter_by(
                id_empresa=row['id_empresa'],
                data_fim=row['data_fim'],
                tempo_analisado=12)
            if query.first() is None:
                session.execute(indicators_table.insert().values(
                    row.to_dict()
                ))
            else:
                session.execute(indicators_table.update().values(
                    row.to_dict(),
                ).where(
                    indicators_table.c.id_empresa == row['id_empresa'],
                    indicators_table.c.data_fim == row['data_fim'],
                    indicators_table.c.tempo_analisado == 12
                ))
            session.commit()
            logger.info(f"Registro salvo/atualizado para empresa {row['id_empresa']} e data_fim {row['data_fim']}")
        except Exception as e:
            print(f"Erro ao salvar no banco: {e}")
            session.rollback()



def refresh_indicators(code_cvm):
    try:
        db_config = 'postgresql+psycopg2://admin:admin_password@db:5432/meu_banco'
        c = CalculadoraIndicadoresFinanceiros(db_config, int(code_cvm))
        df_resultados = c.executar()
        df_resultados = df_resultados.dropna(subset=df_resultados.columns.difference(['data_fim', 'id_empresa']), how='all')
        print(df_resultados.head())
        df_resultados = c.calculo_indicadores(df_resultados)
        print(df_resultados.head())
        _save_results_to_db(df_resultados)
    except Exception as e:
        logger.error(f"Erro ao atualizar indicadores para CVM {code_cvm}: {str(e)}")
        raise

# Definindo a DAG com configurações aprimoradas
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
    'b3_data_extraction',
    default_args=default_args,
    description='Pipeline otimizado para buscar e processar relatórios da B3',
    schedule_interval = '@monthly',
    catchup=False,
    max_active_runs=1,
    concurrency=8,  # Controlar a concorrência
    tags=['b3', 'reports', 'financial_data'],
)

# Parametrização da DAG
from airflow.models import Variable

# Definir variáveis da DAG que podem ser ajustadas via UI do Airflow
empresa = Variable.get("b3_empresa", default_var="TTEN")
ano = int(Variable.get("b3_ano", default_var="2024"))



# Definir as tasks com XComs claramente definidos
t1 = PythonOperator(
    task_id='get_company_info',
    python_callable=get_company_info,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='get_structured_reports',
    python_callable=get_structured_reports,
    op_kwargs={'code_cvm': "{{ task_instance.xcom_pull(task_ids='get_company_info')['code_cvm'] }}", 
               'year': "{{ task_instance.xcom_pull(task_ids='get_company_info')['ano'] }}"},
    provide_context=True,
    dag=dag,
)

t21 = PythonOperator(
    task_id='get_structured_reports_next_year',
    python_callable=get_structured_reports,
    op_kwargs={'code_cvm': "{{ task_instance.xcom_pull(task_ids='get_company_info')['code_cvm'] }}", 
               'year': "{{ task_instance.xcom_pull(task_ids='get_company_info')['ano'] + 1 }}"},
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='extract_url_report',
    python_callable=extract_url_search,
    op_kwargs={'data_json': "{{ task_instance.xcom_pull(task_ids='get_structured_reports_next_year') | tojson }}"},
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='extract_dfp_links',
    python_callable=extract_links_from_select,
    op_kwargs={'url_report': "{{ task_instance.xcom_pull(task_ids='extract_url_report') }}"},
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='extract_itr_links',
    python_callable=get_all_itrs_links,
    op_kwargs={'data_json': "{{ task_instance.xcom_pull(task_ids='get_structured_reports') | tojson }}"},
    provide_context=True,
    dag=dag,
)

t6 = PythonOperator(
    task_id='fetch_itr_data',
    python_callable=fetch_itr_data,
    op_kwargs={
        'url_report': "{{ task_instance.xcom_pull(task_ids='extract_url_report')}}",
        'dfp_links': "{{ task_instance.xcom_pull(task_ids='extract_dfp_links') | tojson }}",
        'itr_links': "{{ task_instance.xcom_pull(task_ids='extract_itr_links') | tojson }}",
        'company_name': "{{ task_instance.xcom_pull(task_ids='get_company_info')['empresa'] }}",
        'year': "{{ task_instance.xcom_pull(task_ids='get_company_info')['ano'] | int }}",
        'code_cvm': "{{ task_instance.xcom_pull(task_ids='get_company_info')['code_cvm'] }}"
    },
    provide_context=True,
    dag=dag,
)

t7 = PythonOperator(
    task_id='send_data_to_api',
    python_callable=send_data_to_api,
    op_kwargs={
        'data': t6.output,
        'company_name': "{{ task_instance.xcom_pull(task_ids='get_company_info')['trading_name'] }}",
        'code_cvm': "{{ task_instance.xcom_pull(task_ids='get_company_info')['code_cvm'] }}"
    },
    provide_context=True,
    dag=dag,
)

t8 = PythonOperator(
    task_id='refresh_indicators',
    python_callable=refresh_indicators,
    op_kwargs={'code_cvm': "{{ task_instance.xcom_pull(task_ids='get_company_info')['code_cvm'] }}"},
    provide_context=True,
    dag=dag,
    trigger_rule='all_done',  # Executa mesmo se tarefas anteriores falharem
)

# Definir as dependências das tarefas
t1 >> [t2, t21]
t2 >> t5
t21 >> t3
t3 >> t4
[t4, t5] >> t6
t6 >> t7
t7 >> t8