"""
DAG que carrega dados da CVM (DFP/ITR) de 2011 até 2024, rodando apenas uma vez.
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import requests
import zipfile
from io import BytesIO, StringIO
import pandas as pd
import numpy as np
import datetime

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# ----------------------------
# CONEXÃO COM O BANCO
# ----------------------------
engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
Session = sessionmaker(bind=engine)
session = Session()
metadata = MetaData(bind=engine)

empresa = Table('empresa', metadata, autoload_with=engine)
relatorio = Table('relatorio', metadata, autoload_with=engine)
dados_relatorio = Table('dados_relatorio', metadata, autoload_with=engine)

# ----------------------------
# FUNÇÕES PARA BUSCAR ARQUIVOS
# ----------------------------
def get_cvm_data_by_year(year, tipo='ITR'):
    """
    Busca dados .zip (ITR ou DFP) da CVM por ano e retorna 4 DataFrames:
      - Balanço Patrimonial Ativo (BPA)
      - Balanço Patrimonial Passivo (BPP)
      - Demonstração do Resultado (DRE)
      - Demonstração do Fluxo de Caixa Método Indireto (DFC_MI)
    """
    try:
        base_url = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC"
        # ITR ou DFP
        url = f"{base_url}/{tipo}/DADOS/{tipo.lower()}_cia_aberta_{year}.zip"

        response = requests.get(url)
        response.raise_for_status()

        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            df_bpa = pd.read_csv(
                StringIO(zf.read(f"{tipo.lower()}_cia_aberta_BPA_con_{year}.csv").decode("latin1")),
                sep=";",
                encoding="latin1",
                low_memory=False
            )
            df_bpp = pd.read_csv(
                StringIO(zf.read(f"{tipo.lower()}_cia_aberta_BPP_con_{year}.csv").decode("latin1")),
                sep=";",
                encoding="latin1",
                low_memory=False
            )
            df_dre = pd.read_csv(
                StringIO(zf.read(f"{tipo.lower()}_cia_aberta_DRE_con_{year}.csv").decode("latin1")),
                sep=";",
                encoding="latin1",
                low_memory=False
            )
            df_dfc_mi = pd.read_csv(
                StringIO(zf.read(f"{tipo.lower()}_cia_aberta_DFC_MI_con_{year}.csv").decode("latin1")),
                sep=";",
                encoding="latin1",
                low_memory=False
            )

        # Ajuste de nome de coluna extra para consistência
        df_dfc_mi["GRUPO_DFP"] = "DF Consolidado - Demonstração do Fluxo de Caixa"
        return df_bpa, df_bpp, df_dre, df_dfc_mi

    except Exception as e:
        print(f"Erro ao baixar {tipo} para o ano {year}: {e}")
        return None, None, None, None

# ----------------------------
# FUNÇÃO DE INSERÇÃO NO BANCO
# ----------------------------
def insert_data_v2(df_data, att_time):
    try:
        try:
            df_data['CD_CVM'] = df_data['CD_CVM'].astype(str).str.replace(',', '', regex=True).astype(float).astype(int)
        except Exception as e:
            print("Erro ao converter CD_CVM:", e)
            print("Valores problemáticos:", df_data[~df_data['CD_CVM'].astype(str).str.replace(',', '', regex=True).str.isnumeric()])
        
        if 'DT_INI_EXERC' in df_data.columns:
            df_data['DT_INI_EXERC'] = df_data['DT_INI_EXERC'].where(pd.notna(df_data['DT_INI_EXERC']), None)
        else:
            df_data['DT_INI_EXERC'] = None
        
        try:
            df_data['CD_CONTA'] = df_data['CD_CONTA'].astype(str).str.replace('.', '').str.replace(',', '', regex=True).astype(int)
        except Exception as e:
            print("Erro ao converter CD_CONTA:", e)
            print("Valores problemáticos:", df_data[~df_data['CD_CONTA'].astype(str).str.replace('.', '').str.replace(',', '', regex=True).str.isnumeric()])

                
        print("Inserindo dados para o relatório:", df_data['GRUPO_DFP'].unique()[0])
        
        df_data['DT_FIM_EXERC'] = df_data['DT_FIM_EXERC'].where(pd.notna(df_data['DT_FIM_EXERC']), None)
        df_data['VL_CONTA'] = df_data['VL_CONTA'].apply(lambda x: float(x) if pd.notna(x) else 0.0)
        
        unique_cvms = [int(x) for x in df_data['CD_CVM'].unique()]
        
        existing_empresas = {int(e.id_empresa) for e in session.query(empresa.c.id_empresa).filter(
            empresa.c.id_empresa.in_(unique_cvms)).all()}
        
        empresas_to_insert = []
        
        for code_cvm in unique_cvms:
            if int(code_cvm) not in existing_empresas:
                try:
                    empresas_to_insert.append({
                        "id_empresa": int(code_cvm), 
                        "nome_empresa": str(df_data[df_data['CD_CVM'] == code_cvm].iloc[0]['DENOM_CIA'])
                    })
                except Exception as e:
                    print(f"Erro ao preparar empresa {code_cvm}: {e}")
        
        if empresas_to_insert:
            try:
                from sqlalchemy.dialects.postgresql import insert
                stmt = insert(empresa).values(empresas_to_insert)
                stmt = stmt.on_conflict_do_nothing(index_elements=['id_empresa'])
                session.execute(stmt)
                session.commit()
            except Exception as e:
                print("Erro ao inserir empresas:", e)
                session.rollback()  # Garante que o erro não afete as próximas operações

    except Exception as e:
        print("Erro no pré-processamento:", e)
        return  # Evita continuar se houver erro crítico

    try:
        relatorios_data = []
        relatorios_keys = set()
        
        for code_cvm in unique_cvms:
            df_group = df_data[df_data['CD_CVM'] == code_cvm]
            nome_relatorio = str(df_group.iloc[0]["GRUPO_DFP"].split(" - ")[1])
            
            group_columns = ['DT_FIM_EXERC'] if df_data['DT_INI_EXERC'].isnull().all() else ['DT_INI_EXERC', 'DT_FIM_EXERC']
            date_combinations = df_group[group_columns].drop_duplicates()
            
            for _, date_row in date_combinations.iterrows():
                data_inicio = date_row['DT_INI_EXERC'] if 'DT_INI_EXERC' in date_row else None
                data_fim = date_row['DT_FIM_EXERC']
                
                key = (nome_relatorio, int(code_cvm), data_inicio, data_fim)
                if key not in relatorios_keys:
                    relatorios_data.append({
                        "tipo_relatorio": nome_relatorio,
                        "id_empresa": int(code_cvm),
                        "data_inicio": data_inicio,
                        "data_fim": data_fim,
                        'ultima_atualizacao': att_time
                    })
                    relatorios_keys.add(key)
        
        if relatorios_data:
            try:
                from sqlalchemy.dialects.postgresql import insert
                stmt = insert(relatorio).values(relatorios_data)
                stmt = stmt.on_conflict_do_nothing(index_elements=['tipo_relatorio', 'id_empresa', 'data_inicio', 'data_fim'])
                session.execute(stmt)
                session.commit()
            except Exception as e:
                print("Erro ao inserir relatórios:", e)
                session.rollback()

    except Exception as e:
        print("Erro ao processar relatórios:", e)

    try:
        inserted_relatorios = {}
        for r in session.query(relatorio).filter(relatorio.c.id_empresa.in_(unique_cvms)).all():
            data_inicio = r.data_inicio.strftime('%Y-%m-%d') if r.data_inicio else None
            data_fim = r.data_fim.strftime('%Y-%m-%d') if r.data_fim else None
            key = (r.tipo_relatorio, int(r.id_empresa), data_inicio, data_fim)
            inserted_relatorios[key] = int(r.id_relatorio)

        dados_relatorios_data = {}

        for code_cvm in unique_cvms:
            df_group = df_data[df_data['CD_CVM'] == code_cvm]
            nome_relatorio = str(df_group.iloc[0]["GRUPO_DFP"].split(" - ")[1])

            group_columns = ['DT_FIM_EXERC'] if df_data['DT_INI_EXERC'].isnull().all() else ['DT_INI_EXERC', 'DT_FIM_EXERC']
            grouped = df_group.groupby(group_columns)

            for group_key, group in grouped:
                if len(group_columns) == 1:
                    data_fim = group_key
                    data_inicio = None
                else:
                    data_inicio, data_fim = group_key

                data_inicio = data_inicio[0] if isinstance(data_inicio, tuple) else data_inicio
                data_fim = data_fim[0] if isinstance(data_fim, tuple) else data_fim

                if isinstance(data_inicio, pd.Timestamp):
                    data_inicio = data_inicio.strftime('%Y-%m-%d')

                if isinstance(data_fim, pd.Timestamp):
                    data_fim = data_fim.strftime('%Y-%m-%d')

                relatorio_id = inserted_relatorios.get((nome_relatorio, int(code_cvm), data_inicio, data_fim))

                if relatorio_id:
                    for _, row in group.iterrows():
                        descricao = str(row["DS_CONTA"])
                        valor = float(str(row["VL_CONTA"]).replace(",", '.')) if pd.notna(row["VL_CONTA"]) else 0.0
                        codigo_conta = int(str(row["CD_CONTA"]).replace(".", ""))

                        key = (relatorio_id, codigo_conta)

                        dados_relatorios_data[key] = {
                            "id_relatorio": relatorio_id,
                            "codigo_conta": codigo_conta,
                            "descricao": descricao,
                            "valor": valor
                        }

        if dados_relatorios_data:
            try:
                from sqlalchemy.dialects.postgresql import insert
                dados_relatorios_data = list(dados_relatorios_data.values())
                stmt = insert(dados_relatorio).values(dados_relatorios_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id_relatorio', 'codigo_conta'],
                    set_={'descricao': stmt.excluded.descricao, 'valor': stmt.excluded.valor}
                )
                session.execute(stmt)
                session.commit()
                print("Dados inseridos com sucesso!")
            except Exception as e:
                print("Erro ao inserir dados:", e)
                session.rollback()

    except Exception as e:
        print("Erro final:", e)
        session.rollback()


# ----------------------------
# FUNÇÃO PRINCIPAL P/ UM ANO
# ----------------------------
def process_year_dfp(year):
    df_dfp_bpa, df_dfp_bpp, df_dfp_dre, df_dfp_dfc_mi = get_cvm_data_by_year(year, tipo='DFP')
    att_time = datetime.datetime.now()
    insert_data_v2(df_dfp_bpa, att_time)
    insert_data_v2(df_dfp_bpp, att_time)
    insert_data_v2(df_dfp_dre, att_time)
    insert_data_v2(df_dfp_dfc_mi, att_time)
    
    print(f"DFPs processados para o ano {year}")
    
def process_year_itr(year):
    df_itr_bpa, df_itr_bpp, df_itr_dre, df_itr_dfc_mi = get_cvm_data_by_year(year, tipo='ITR')
    att_time = datetime.datetime.now()
    insert_data_v2(df_itr_bpa, att_time)
    insert_data_v2(df_itr_bpp, att_time)
    insert_data_v2(df_itr_dre, att_time)
    insert_data_v2(df_itr_dfc_mi, att_time)
    
    print(f"ITRs processados para o ano {year}")


# ----------------------------
# DEFINIÇÃO DA DAG
# ----------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # Data retroativa para permitir execução imediata
    'retries': 0
}

from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='cvm_data_sequencial_paralelo',
    default_args=default_args,
    schedule_interval=None,  # Não agendar, executa on demand
    catchup=False
) as dag:

    previous_tg = None
    for year in range(2011, datetime.datetime.now().year + 1):
        with TaskGroup(group_id=f"processo_{year}") as tg_year:
            dfp_task = PythonOperator(
                task_id=f"dfp_{year}",
                python_callable=process_year_dfp,
                op_kwargs={'year': year}
            )
            itr_task = PythonOperator(
                task_id=f"itr_{year}",
                python_callable=process_year_itr,
                op_kwargs={'year': year}
            )
            # Dentro do mesmo TaskGroup, dfp_task e itr_task rodam em paralelo
            # pois não há dependência entre eles

        # Se existir um TaskGroup anterior, define a dependência para rodar este só depois de concluir o anterior
        if previous_tg:
            previous_tg >> tg_year

        previous_tg = tg_year