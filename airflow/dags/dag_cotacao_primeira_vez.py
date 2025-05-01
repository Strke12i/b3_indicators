import pandas as pd
import requests
import zipfile
from io import BytesIO, TextIOWrapper
from datetime import datetime, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, inspect, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert # Importar insert
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os
import tempfile # Importar tempfile

# Suppress insecure request warnings
warnings.simplefilter('ignore', InsecureRequestWarning)

DATABASE_URL = "postgresql+psycopg2://admin:admin_password@db:5432/meu_banco"
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
metadata = MetaData(bind=engine)

# --- Definições de colunas, mercados, etc. (mantidas como no original) ---
FIELD_SIZES = {
    'TIPO_DE_REGISTRO': 2, 'DATA_DO_PREGAO': 8, 'CODIGO_BDI': 2, 'CODIGO_DE_NEGOCIACAO': 12,
    'TIPO_DE_MERCADO': 3, 'NOME_DA_EMPRESA': 12, 'ESPECIFICACAO_DO_PAPEL': 10,
    'PRAZO_EM_DIAS_DO_MERCADO_A_TERMO': 3, 'MOEDA_DE_REFERENCIA': 4, 'PRECO_DE_ABERTURA': 13,
    'PRECO_MAXIMO': 13, 'PRECO_MINIMO': 13, 'PRECO_MEDIO': 13, 'PRECO_ULTIMO_NEGOCIO': 13,
    'PRECO_MELHOR_OFERTA_DE_COMPRA': 13, 'PRECO_MELHOR_OFERTA_DE_VENDAS': 13,
    'NUMERO_DE_NEGOCIOS': 5, 'QUANTIDADE_NEGOCIADA': 18, 'VOLUME_TOTAL_NEGOCIADO': 18,
    'PRECO_DE_EXERCICIO': 13, 'INDICADOR_DE_CORRECAO_DE_PRECOS': 1, 'DATA_DE_VENCIMENTO': 8,
    'FATOR_DE_COTACAO': 7, 'PRECO_DE_EXERCICIO_EM_PONTOS': 13, 'CODIGO_ISIN': 12,
    'NUMERO_DE_DISTRIBUICAO': 3
}
DATE_COLUMNS = ("DATA_DO_PREGAO", "DATA_DE_VENCIMENTO")
FLOAT32_COLUMNS = ('PRECO_DE_ABERTURA', 'PRECO_MAXIMO', 'PRECO_MINIMO', 'PRECO_MEDIO',
                   'PRECO_ULTIMO_NEGOCIO', 'PRECO_MELHOR_OFERTA_DE_COMPRA',
                   'PRECO_MELHOR_OFERTA_DE_VENDAS', 'PRECO_DE_EXERCICIO',
                   'PRECO_DE_EXERCICIO_EM_PONTOS')
FLOAT64_COLUMNS = ("VOLUME_TOTAL_NEGOCIADO", "QUANTIDADE_NEGOCIADA")
# ... (outras definições de colunas e dicionários mantidas) ...
MARKETS = {
    '010':'VISTA', '012':'EXERCICIO_DE_OPCOES_DE_COMPRA', '013':'EXERCÍCIO_DE_OPCOES_DE_VENDA',
    '017':'LEILAO', '020':'FRACIONARIO', '030':'TERMO', '050':'FUTURO_COM_RETENCAO_DE_GANHO',
    '060':'FUTURO_COM_MOVIMENTACAO_CONTINUA', '070':'OPCOES_DE_COMPRA', '080':'OPCOES_DE_VENDA'
}
INDOPC = {'0':'0', '1':'US$', '2':"TJLP", '8':"IGPM", '9':"URV"}
CODBDI = {
    '00':'0', '02':"LOTE_PADRAO", '05':"SANCIONADAS PELOS REGULAMENTOS BMFBOVESPA",
    '06':"CONCORDATARIAS", '07':"RECUPERACAO_EXTRAJUDICIAL", '08':"RECUPERAÇÃO_JUDICIAL",
    '09':"REGIME_DE_ADMINISTRACAO_ESPECIAL_TEMPORARIA", '10':"DIREITOS_E_RECIBOS",
    '11':"INTERVENCAO", '12':"FUNDOS_IMOBILIARIOS", '13':'13', '14':"CERT.INVEST/TIT.DIV.PUBLICA",
    '18':"OBRIGACÕES", '22':"BÔNUS(PRIVADOS)", '26':"APOLICES/BÔNUS/TITULOS PUBLICOS",
    '32':"EXERCICIO_DE_OPCOES_DE_COMPRA_DE_INDICES", '33':"EXERCICIO_DE_OPCOES_DE_VENDA_DE_INDICES",
    '34':'34', '35':'35', '36':'36', '37':'37', '38':"EXERCICIO_DE_OPCOES_DE_COMPRA",
    '42':"EXERCICIO_DE_OPCOES_DE_VENDA", '46':"LEILAO_DE_NAO_COTADOS", '48':"LEILAO_DE_PRIVATIZACAO",
    '49':"LEILAO_DO_FUNDO_RECUPERACAO_ECONOMICA_ESPIRITO_SANTO", '50':"LEILAO", '51':"LEILAO_FINOR",
    '52':"LEILAO_FINAM", '53':"LEILAO_FISET", '54':"LEILAO_DE_ACÕES_EM_MORA",
    '56':"VENDAS_POR_ALVARA_JUDICIAL", '58':"OUTROS", '60':"PERMUTA_POR_ACÕES", '61':"META",
    '62':"MERCADO_A_TERMO", '66':"DEBENTURES_COM_DATA_DE_VENCIMENTO_ATE_3_ANOS",
    '68':"DEBENTURES_COM_DATA_DE_VENCIMENTO_MAIOR_QUE_3_ANOS", '70':"FUTURO_COM_RETENCAO_DE_GANHOS",
    '71':"MERCADO_DE_FUTURO", '74':"OPCOES_DE_COMPRA_DE_INDICES", '75':"OPCOES_DE_VENDA_DE_INDICES",
    '78':"OPCOES_DE_COMPRA", '82':"OPCOES_DE_VENDA", '83':"BOVESPAFIX", '84':"SOMA_FIX",
    '90':"TERMO_VISTA_REGISTRADO", '96':"MERCADO_FRACIONARIO", '99':"TOTAL_GERAL"
}
# --- Fim das definições mantidas ---

BASE_URL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST"

def insert_chunk_into_database(df_chunk, existing_tickers):
    """Insert a processed chunk into the database."""
    if df_chunk is None or df_chunk.empty:
        print("Chunk vazio, nada a inserir.")
        return 0

    session = Session()
    inserted_count = 0
    try:
        cotacao = Table('cotacao', metadata, autoload_with=engine)
        records = []
        for _, row in df_chunk.iterrows():
            # A verificação do ticker agora acontece aqui, usando o set passado
            if row['CODIGO_ISIN'] in existing_tickers:
                records.append({
                    'codigo_isin': row['CODIGO_ISIN'],
                    'data_pregao': row['DATA_DO_PREGAO'],
                    'abertura': row['PRECO_DE_ABERTURA'],
                    'fechamento': row['PRECO_ULTIMO_NEGOCIO'],
                    'numero_de_negocios': row['NUMERO_DE_NEGOCIOS'],
                    'quantidade_negociada': row['QUANTIDADE_NEGOCIADA'],
                    'volume_negociado': row['VOLUME_TOTAL_NEGOCIADO']
                })

        if records:
            stmt = insert(cotacao).values(records)
            # Usar on_conflict_do_nothing para ignorar duplicatas (mesmo ISIN/data_pregao)
            stmt = stmt.on_conflict_do_nothing(index_elements=['codigo_isin', 'data_pregao'])
            result = session.execute(stmt)
            session.commit()
            inserted_count = len(records) # Aproximação, on_conflict pode reduzir
            print(f"Inserido/Ignorado ~{inserted_count} registros do chunk na tabela cotacao.")
        else:
            print("Nenhum ticker correspondente encontrado no chunk para inserção.")

    except Exception as e:
        session.rollback()
        print(f"Erro ao inserir chunk no banco de dados: {e}")
        # Re-raise a exceção para que o Airflow marque a tarefa como falha, se desejado
        # raise e
    finally:
        session.close()
    return inserted_count # Retorna o número de registros tentados

def process_dataframe_chunk(df_chunk):
    """Process a dataframe chunk with the provided transformations."""
    if df_chunk.empty:
        return df_chunk

    # --- NOVO: Filtrar linha do trailer (TIPO_DE_REGISTRO = '99') ---
    # Assume que a coluna 'TIPO_DE_REGISTRO' é a primeira e indica o trailer com '99'
    # Faça isso ANTES de qualquer outra conversão ou filtro
    df_chunk = df_chunk[df_chunk['TIPO_DE_REGISTRO'] != '99'].copy()
    if df_chunk.empty:
         print("Chunk ficou vazio após remover linha de trailer.")
         return df_chunk
    # --- FIM DO NOVO FILTRO ---

    # Aplica transformações que não dependem do DataFrame inteiro
    # df_chunk["TIPO_DE_MERCADO"] = df_chunk["TIPO_DE_MERCADO"].apply(lambda x: MARKETS.get(x, x)) # Removido pois não é usado no filtro/insert
    # df_chunk["INDICADOR_DE_CORRECAO_DE_PRECOS"] = df_chunk["INDICADOR_DE_CORRECAO_DE_PRECOS"].apply(lambda x: INDOPC.get(x, x)) # Removido
    df_chunk["CODIGO_BDI"] = df_chunk["CODIGO_BDI"].apply(lambda x: CODBDI.get(x, x))

    # Filtra ANTES de converter tipos para economizar processamento
    df_processed = df_chunk[df_chunk['CODIGO_BDI'] == "LOTE_PADRAO"].copy() # Usar .copy() para evitar SettingWithCopyWarning

    if df_processed.empty:
        # print("Chunk ficou vazio após filtrar por CODIGO_BDI.") # Log opcional
        return df_processed # Retorna vazio se o filtro eliminar tudo

    # Aplica conversões de tipo apenas nas colunas necessárias e no chunk filtrado
    for col in FLOAT32_COLUMNS:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype(float) / 100
    for col in FLOAT64_COLUMNS:
        if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0).astype(float)
    for col in DATE_COLUMNS:
         if col in df_processed.columns:
            df_processed[col] = df_processed[col].astype(str).apply(lambda x: pd.NaT if x == "99991231" or not x.isdigit() else x)
            df_processed[col] = pd.to_datetime(df_processed[col], format='%Y%m%d', errors='coerce')

    # Seleciona as colunas finais necessárias para inserção
    final_columns = ['DATA_DO_PREGAO', 'PRECO_DE_ABERTURA', 'PRECO_ULTIMO_NEGOCIO',
                     'NUMERO_DE_NEGOCIOS', 'QUANTIDADE_NEGOCIADA', 'VOLUME_TOTAL_NEGOCIADO',
                     'CODIGO_ISIN']
    # Verifica se todas as colunas existem antes de selecionar
    final_columns = [col for col in final_columns if col in df_processed.columns]
    df_processed = df_processed[final_columns]


    # Converte tipos numéricos que podem ter vindo como object após leitura
    numeric_cols_for_insert = ['PRECO_DE_ABERTURA', 'PRECO_ULTIMO_NEGOCIO', 'NUMERO_DE_NEGOCIOS',
                               'QUANTIDADE_NEGOCIADA', 'VOLUME_TOTAL_NEGOCIADO']
    for col in numeric_cols_for_insert:
         if col in df_processed.columns:
            df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce').fillna(0)

    # Garante tipo inteiro para colunas que devem ser inteiras no BD
    int_cols = ['NUMERO_DE_NEGOCIOS']
    for col in int_cols:
        if col in df_processed.columns:
             # Adiciona tratamento para NaN antes de converter para int
            df_processed[col] = df_processed[col].fillna(0).astype(int)


    return df_processed


def download_process_insert_chunked(url_suffix, chunk_size=100000):
    """Download, process IN CHUNKS, and insert data into the database."""
    url = f"{BASE_URL}{url_suffix}"
    tmp_zip_path = None
    tmp_txt_path = None
    total_inserted = 0
    session = Session() # Criar sessão aqui para buscar tickers uma vez
    try:
        # 0. Buscar tickers existentes ANTES de começar o processamento do arquivo
        print("Buscando tickers existentes no banco de dados...")
        existing_tickers = set(session.execute("SELECT codigo_isin FROM ticker").scalars().all())
        print(f"Encontrados {len(existing_tickers)} tickers existentes.")
        if not existing_tickers:
             print("AVISO: Nenhum ticker encontrado na tabela 'ticker'. Nenhuma cotação será inserida.")
             # Você pode decidir parar aqui se for um requisito ter tickers pré-existentes
             # return False

        # 1. Download para arquivo temporário (evita manter 80MB+ em memória)
        print(f"Downloading {url}...")
        response = requests.get(url, verify=False, stream=True)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip_file:
            for chunk in response.iter_content(chunk_size=8192 * 16): # Aumentar chunk de download
                tmp_zip_file.write(chunk)
            tmp_zip_path = tmp_zip_file.name
        print(f"Downloaded to temporary zip file: {tmp_zip_path}")

        # 2. Extrair o TXT para outro arquivo temporário
        file_name_in_zip = url_suffix.replace('.ZIP', '.TXT')
        file_name_in_zip = "COTAHIST" + file_name_in_zip
        print(f"Extracting {file_name_in_zip} from zip...")

        with zipfile.ZipFile(tmp_zip_path, 'r') as zf:
            # Verificar se o arquivo existe no ZIP
            if file_name_in_zip not in zf.namelist():
                print(f"ERRO: Arquivo {file_name_in_zip} não encontrado dentro de {tmp_zip_path}")
                # Tentar listar arquivos para depuração
                print(f"Arquivos no ZIP: {zf.namelist()}")
                # Procurar por nomes semelhantes (ex: variação de ano/mês/dia)
                possible_matches = [name for name in zf.namelist() if '.TXT' in name.upper()]
                print(f"Possíveis arquivos TXT no ZIP: {possible_matches}")
                return False # Falha

            with zf.open(file_name_in_zip) as compressed_file:
                # Usar TextIOWrapper para decodificar durante a extração para o temp file
                with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='latin1', suffix=".txt") as tmp_txt_file:
                    # Envolve o stream binário com TextIOWrapper para decodificação correta
                    with TextIOWrapper(compressed_file, encoding='latin1') as text_reader:
                        # Copia o conteúdo decodificado para o arquivo temporário
                        buffer_size = 8192 * 16
                        while True:
                            chunk_content = text_reader.read(buffer_size)
                            if not chunk_content:
                                break
                            tmp_txt_file.write(chunk_content)
                    tmp_txt_path = tmp_txt_file.name
        print(f"Extracted to temporary text file: {tmp_txt_path}")

        # 3. Ler e processar o arquivo TXT em chunks
        print(f"Processing {tmp_txt_path} in chunks of size {chunk_size}...")
        reader = pd.read_fwf(tmp_txt_path, header=None, names=list(FIELD_SIZES.keys()),
                            widths=list(FIELD_SIZES.values()), encoding='latin1',
                            skiprows=1, # Manter skiprows=1 para pular o cabeçalho
                            # skipfooter=1, # REMOVER ESTA LINHA
                            chunksize=chunk_size, iterator=True,
                            dtype=str) # Manter dtype=str

        chunk_num = 0
        for df_chunk in reader:
            chunk_num += 1
            print(f"Processing chunk {chunk_num}...")
            processed_chunk = process_dataframe_chunk(df_chunk)
            if processed_chunk is not None and not processed_chunk.empty:
                print(f"Inserting chunk {chunk_num} ({len(processed_chunk)} rows after processing)...")
                inserted = insert_chunk_into_database(processed_chunk, existing_tickers)
                total_inserted += inserted
            else:
                print(f"Chunk {chunk_num} resulted in empty dataframe after processing.")

        print(f"Finished processing file {url_suffix}. Total records attempted insertion: {total_inserted}")
        return True # Sucesso

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False # Falha
    except zipfile.BadZipFile:
        print(f"Error: Bad ZIP file downloaded from {url}. It might be corrupted or incomplete.")
        return False # Falha
    except KeyError as e:
         print(f"Error processing {url}: Likely a missing column during processing - {e}")
         return False # Falha
    except Exception as e:
        import traceback
        print(f"General error processing {url}: {e}")
        print(traceback.format_exc()) # Imprime stack trace para depuração
        return False # Falha
    finally:
        # 4. Limpeza dos arquivos temporários
        if tmp_txt_path and os.path.exists(tmp_txt_path):
            os.remove(tmp_txt_path)
            print(f"Removed temporary text file: {tmp_txt_path}")
        if tmp_zip_path and os.path.exists(tmp_zip_path):
            os.remove(tmp_zip_path)
            print(f"Removed temporary zip file: {tmp_zip_path}")
        if session:
            session.close() # Fecha a sessão usada para buscar tickers


# --- Funções de Geração de Sufixos (mantidas como no original) ---
def get_years_range():
    current_year = datetime.now().year
    return range(2024, current_year) # Ajustado para começar de 2021 como no original

def get_months_current_year():
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    # Gera para meses anteriores ao atual no ano corrente
    return [f"_M{month:02d}{current_year}.ZIP" for month in range(1, current_month)]

def get_days_current_month():
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    current_day = current_date.day
    # Gera para todos os dias desde o dia 1 até o dia anterior ao atual no mês corrente
    # Ajuste: B3 gera D-1. Se rodar dia 18, pega até dia 17.
    # Se rodar na segunda, pode precisar pegar sexta, sábado, domingo. A lógica B3 é complexa.
    # Simplificação: Pega até o dia atual - 1. Se for dia 1, não pega nenhum.
    if current_day <= 1:
        return []
    return [f"_D{day:02d}{current_month:02d}{current_year}.ZIP" for day in range(1, current_day)]


# --- Funções de Orquestração (Modificadas) ---
def process_year(**kwargs):
    print("Iniciando processamento anual...")
    success_count = 0
    fail_count = 0
    for year in get_years_range():
        url_suffix = f"_A{year}.ZIP"
        print(f"Processando dados para o ano {year} (Arquivo: {url_suffix})")
        success = download_process_insert_chunked(url_suffix)
        if success:
            print(f"Dados do ano {year} processados com sucesso.")
            success_count += 1
        else:
            print(f"Falha ao processar dados do ano {year}.")
            fail_count += 1
    print(f"Processamento anual concluído. Sucessos: {success_count}, Falhas: {fail_count}")
    if fail_count > 0:
        # Opcional: Lançar exceção para falhar a task do Airflow se algum arquivo falhar
        # raise ValueError(f"{fail_count} arquivos anuais falharam ao processar.")
        pass


def process_month(**kwargs):
    print("Iniciando processamento mensal...")
    success_count = 0
    fail_count = 0
    months_suffixes = get_months_current_year()
    if not months_suffixes:
        print("Nenhum mês anterior no ano corrente para processar.")
        return
    for month_suffix in months_suffixes:
        print(f"Processando dados para o mês {month_suffix}")
        success = download_process_insert_chunked(month_suffix)
        if success:
            print(f"Dados do mês {month_suffix} processados com sucesso.")
            success_count += 1
        else:
            print(f"Falha ao processar dados do mês {month_suffix}.")
            fail_count += 1
    print(f"Processamento mensal concluído. Sucessos: {success_count}, Falhas: {fail_count}")
    if fail_count > 0:
        # raise ValueError(f"{fail_count} arquivos mensais falharam ao processar.")
        pass

def process_day(**kwargs):
    print("Iniciando processamento diário...")
    success_count = 0
    fail_count = 0
    days_suffixes = get_days_current_month()
    if not days_suffixes:
        print("Nenhum dia anterior no mês corrente para processar.")
        return
    for day_suffix in days_suffixes:
        print(f"Processando dados para o dia {day_suffix}")
        success = download_process_insert_chunked(day_suffix)
        if success:
            print(f"Dados do dia {day_suffix} processados com sucesso.")
            success_count += 1
        else:
            print(f"Falha ao processar dados do dia {day_suffix}.")
            fail_count += 1
    print(f"Processamento diário concluído. Sucessos: {success_count}, Falhas: {fail_count}")
    if fail_count > 0:
        # raise ValueError(f"{fail_count} arquivos diários falharam ao processar.")
        pass

# --- Definição da DAG (mantida como no original) ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    # 'execution_timeout': timedelta(hours=2), # Considere adicionar um timeout maior
}

with DAG(
    'dados_cotacao_historica_chunked', # Nome da DAG alterado
    default_args=default_args,
    description='Carrega dados COTA HIST em chunks para economizar memória',
    schedule_interval=None, # Ou '@daily' etc.
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='process_years_chunked',
        python_callable=process_year,
        # op_kwargs={}, # Não são mais necessários kwargs aqui
    )

    t2 = PythonOperator(
        task_id='process_months_chunked',
        python_callable=process_month,
        # op_kwargs={},
    )

    t3 = PythonOperator(
        task_id='process_days_chunked',
        python_callable=process_day,
        # op_kwargs={},
    )

    t1 >> t2 >> t3