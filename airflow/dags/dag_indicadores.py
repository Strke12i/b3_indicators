from datetime import datetime
from airflow import DAG
from airflow.decorators import task

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
import logging

# Importe sua classe CalculadoraIndicadoresFinanceiros
from calculadoraIndicadoresFinanceiros import CalculadoraIndicadoresFinanceiros

# Configuração do logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

db_config = 'postgresql+psycopg2://admin:admin_password@db:5432/meu_banco'
engine = create_engine(db_config)

engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
Session = sessionmaker(bind=engine)
session = Session()

# Define a tabela
metadata = MetaData(bind=engine)
indicators_table = Table('indicadores', metadata, autoload_with=engine)

with DAG(
    dag_id="atualiza_indicadores",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_tasks=1  # Ajuste conforme necessário
) as dag:

    @task
    def buscar_empresas_com_relatorios_atualizados_hoje():
        """
        Busca os IDs das empresas que tiveram relatório atualizado hoje.
        """
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT DISTINCT id_empresa
                    FROM relatorio
                    WHERE ultima_atualizacao >= CURRENT_DATE - INTERVAL '1 day'
                """)
                result = conn.execute(query).fetchall()
            empresas_ids = [row[0] for row in result]
            logger.info(f"Encontradas {len(empresas_ids)} empresas: {empresas_ids}")
            return empresas_ids
        except Exception as e:
            logger.error(f"Erro ao buscar empresas: {str(e)}")
            raise

    @task
    def atualizar_indicadores(id_empresa):
        """
        Calcula e atualiza os indicadores para uma empresa.
        """
        db_config = 'postgresql+psycopg2://admin:admin_password@db:5432/meu_banco'
        try:
            logger.info(f"Processando indicadores para a empresa {id_empresa}.")
            calculadora = CalculadoraIndicadoresFinanceiros(db_config, id_empresa)
            calculadora.run(session, indicators_table)
            
        except Exception as e:
            logger.error(f"Erro ao atualizar indicadores de {id_empresa}: {str(e)}")

    # Definir o fluxo da DAG
    empresas = buscar_empresas_com_relatorios_atualizados_hoje()
    atualizar_indicadores.expand(id_empresa=empresas)