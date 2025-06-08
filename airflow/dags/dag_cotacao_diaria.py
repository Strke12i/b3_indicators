import pandas as pd
import requests
import zipfile
from io import BytesIO, TextIOWrapper
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, inspect, MetaData, Table, select, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from urllib3.exceptions import InsecureRequestWarning
import warnings
import os
import tempfile

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

BASE_URL = "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST"

def get_last_quote_date():
    """Obtém a data da última cotação inserida no banco de dados."""
    session = Session()
    try:
        cotacao = Table('cotacao', metadata, autoload_with=engine)
        # Consulta para obter a data mais recente
        query = select(func.max(cotacao.c.data_pregao))
        last_date = session.execute(query).scalar()

        if last_date:
            print(f"Última data de cotação encontrada: {last_date}")
            # Adiciona um dia para começar a partir do próximo dia
            return last_date + timedelta(days=1)
        else:
            print("Nenhuma cotação encontrada no banco. Iniciando a partir de uma data padrão.")
            # Se não houver cotações, inicia a partir de uma data padrão (ex: 01/01/2024)
            return datetime(2024, 1, 1).date()
    except Exception as e:
        print(f"Erro ao obter a última data de cotação: {e}")
        # Em caso de erro, retorna uma data padrão
        return datetime(2024, 1, 1).date()
    finally:
        session.close()

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
    finally:
        session.close()
    return inserted_count

def process_dataframe_chunk(df_chunk):
    """Process a dataframe chunk with the provided transformations."""
    if df_chunk.empty:
        return df_chunk

    # Filtrar linha do trailer (TIPO_DE_REGISTRO = '99')
    df_chunk = df_chunk[df_chunk['TIPO_DE_REGISTRO'] != '99'].copy()
    if df_chunk.empty:
         print("Chunk ficou vazio após remover linha de trailer.")
         return df_chunk

    df_chunk["CODIGO_BDI"] = df_chunk["CODIGO_BDI"].apply(lambda x: CODBDI.get(x, x))

    # Filtra ANTES de converter tipos para economizar processamento
    df_processed = df_chunk[df_chunk['CODIGO_BDI'] == "LOTE_PADRAO"].copy()

    if df_processed.empty:
        return df_processed

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
            df_processed[col] = df_processed[col].fillna(0).astype(int)

    return df_processed

def download_process_insert_chunked(url_suffix, chunk_size=100000):
    """Download, process IN CHUNKS, and insert data into the database."""
    url = f"{BASE_URL}{url_suffix}"
    tmp_zip_path = None
    tmp_txt_path = None
    total_inserted = 0
    session = Session()
    try:
        # Buscar tickers existentes
        print("Buscando tickers existentes no banco de dados...")
        existing_tickers = set(session.execute("SELECT codigo_isin FROM ticker").scalars().all())
        print(f"Encontrados {len(existing_tickers)} tickers existentes.")
        if not existing_tickers:
             print("AVISO: Nenhum ticker encontrado na tabela 'ticker'. Nenhuma cotação será inserida.")

        # Download para arquivo temporário
        print(f"Downloading {url}...")
        response = requests.get(url, verify=False, stream=True)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip_file:
            for chunk in response.iter_content(chunk_size=8192 * 16):
                tmp_zip_file.write(chunk)
            tmp_zip_path = tmp_zip_file.name
        print(f"Downloaded to temporary zip file: {tmp_zip_path}")

        # Extrair o TXT para outro arquivo temporário
        file_name_in_zip = url_suffix.replace('.ZIP', '.TXT')
        file_name_in_zip = "COTAHIST" + file_name_in_zip
        print(f"Extracting {file_name_in_zip} from zip...")

        with zipfile.ZipFile(tmp_zip_path, 'r') as zf:
            # Verificar se o arquivo existe no ZIP
            if file_name_in_zip not in zf.namelist():
                print(f"ERRO: Arquivo {file_name_in_zip} não encontrado dentro de {tmp_zip_path}")
                # Tentar listar arquivos para depuração
                print(f"Arquivos no ZIP: {zf.namelist()}")
                # Procurar por nomes semelhantes
                possible_matches = [name for name in zf.namelist() if '.TXT' in name.upper()]
                print(f"Possíveis arquivos TXT no ZIP: {possible_matches}")
                return False

            with zf.open(file_name_in_zip) as compressed_file:
                with tempfile.NamedTemporaryFile(delete=False, mode='w', encoding='latin1', suffix=".txt") as tmp_txt_file:
                    with TextIOWrapper(compressed_file, encoding='latin1') as text_reader:
                        buffer_size = 8192 * 16
                        while True:
                            chunk_content = text_reader.read(buffer_size)
                            if not chunk_content:
                                break
                            tmp_txt_file.write(chunk_content)
                    tmp_txt_path = tmp_txt_file.name
        print(f"Extracted to temporary text file: {tmp_txt_path}")

        # Ler e processar o arquivo TXT em chunks
        print(f"Processing {tmp_txt_path} in chunks of size {chunk_size}...")
        reader = pd.read_fwf(tmp_txt_path, header=None, names=list(FIELD_SIZES.keys()),
                            widths=list(FIELD_SIZES.values()), encoding='latin1',
                            skiprows=1, chunksize=chunk_size, iterator=True,
                            dtype=str)

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
        return True

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return False
    except zipfile.BadZipFile:
        print(f"Error: Bad ZIP file downloaded from {url}. It might be corrupted or incomplete.")
        return False
    except KeyError as e:
         print(f"Error processing {url}: Likely a missing column during processing - {e}")
         return False
    except Exception as e:
        import traceback
        print(f"General error processing {url}: {e}")
        print(traceback.format_exc())
        return False
    finally:
        # Limpeza dos arquivos temporários
        if tmp_txt_path and os.path.exists(tmp_txt_path):
            os.remove(tmp_txt_path)
            print(f"Removed temporary text file: {tmp_txt_path}")
        if tmp_zip_path and os.path.exists(tmp_zip_path):
            os.remove(tmp_zip_path)
            print(f"Removed temporary zip file: {tmp_zip_path}")
        if session:
            session.close()

def generate_date_suffixes(start_date, end_date=None):
    """
    Gera sufixos de arquivos para datas entre start_date e end_date (ou hoje).
    Retorna uma lista de tuplas (sufixo, tipo) onde tipo é 'year', 'month' ou 'day'.
    """
    if end_date is None:
        end_date = datetime.now().date()

    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    print(f"Gerando sufixos de arquivos de {start_date} até {end_date}")

    suffixes = []

    # Se o intervalo for maior que um ano, adiciona arquivos anuais
    current_year = start_date.year
    while current_year < end_date.year:
        suffixes.append((f"_A{current_year}.ZIP", 'year'))
        current_year += 1

    # Se estamos no mesmo ano, ou para o último ano do intervalo
    if start_date.year == end_date.year:
        # Se o intervalo for maior que um mês, adiciona arquivos mensais
        current_month = start_date.month
        while current_month < end_date.month:
            suffixes.append((f"_M{current_month:02d}{start_date.year}.ZIP", 'month'))
            current_month += 1

        # Para o mês atual ou último mês do intervalo
        if start_date.month == end_date.month:
            # Adiciona arquivos diários
            current_day = start_date.day
            while current_day <= end_date.day:
                # Verifica se a data é válida (não é um dia futuro)
                check_date = date(end_date.year, end_date.month, current_day)
                if check_date <= datetime.now().date():
                    suffixes.append((f"_D{current_day:02d}{end_date.month:02d}{end_date.year}.ZIP", 'day'))
                current_day += 1
    else:
        # Para o primeiro ano, pegamos os meses restantes
        current_month = start_date.month
        while current_month <= 12:
            suffixes.append((f"_M{current_month:02d}{start_date.year}.ZIP", 'month'))
            current_month += 1

        # Para o último ano, pegamos os meses até o atual
        current_month = 1
        while current_month < end_date.month:
            suffixes.append((f"_M{current_month:02d}{end_date.year}.ZIP", 'month'))
            current_month += 1

        # Para o mês atual do último ano
        current_day = 1
        while current_day <= end_date.day:
            # Verifica se a data é válida (não é um dia futuro)
            check_date = date(end_date.year, end_date.month, current_day)
            if check_date <= datetime.now().date():
                suffixes.append((f"_D{current_day:02d}{end_date.month:02d}{end_date.year}.ZIP", 'day'))
            current_day += 1

    return suffixes

def process_incremental_data(**kwargs):
    """
    Processa dados incrementalmente a partir da última data de cotação no banco.
    """
    print("Iniciando processamento incremental de cotações...")

    # Obtém a data da última cotação no banco
    start_date = get_last_quote_date()
    end_date = datetime.now().date() - timedelta(days=1)  # Até ontem (B3 publica com 1 dia de atraso)

    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()

    if start_date > end_date:
        print(f"Dados já atualizados até {start_date}. Nada a processar.")
        return

    print(f"Processando cotações de {start_date} até {end_date}")

    # Gera os sufixos de arquivos a serem processados
    suffixes = generate_date_suffixes(start_date, end_date)

    if not suffixes:
        print("Nenhum arquivo para processar no período especificado.")
        return

    print(f"Arquivos a processar: {len(suffixes)}")

    success_count = 0
    fail_count = 0

    # Processa cada arquivo na ordem: anual, mensal, diário
    for suffix, suffix_type in suffixes:
        print(f"Processando arquivo {suffix} (tipo: {suffix_type})")
        success = download_process_insert_chunked(suffix)

        if success:
            print(f"Arquivo {suffix} processado com sucesso.")
            success_count += 1
        else:
            print(f"Falha ao processar arquivo {suffix}.")
            fail_count += 1

    print(f"Processamento incremental concluído. Sucessos: {success_count}, Falhas: {fail_count}")

    if fail_count > 0:
        print(f"ATENÇÃO: {fail_count} arquivos falharam durante o processamento.")

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dados_cotacao_incremental',
    default_args=default_args,
    description='Carrega dados de cotação incrementalmente a partir da última data no banco',
    schedule_interval='0 6 * * 1-5',  # Executa às 6h em dias úteis (seg-sex)
    catchup=False,
) as dag:

    process_task = PythonOperator(
        task_id='process_incremental_data',
        python_callable=process_incremental_data,
    )