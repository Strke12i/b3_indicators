from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from CalculadoraIndicadoresFinanceiros import CalculadoraIndicadoresFinanceiros
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
import pandas as pd
import logging

logger = logging.getLogger(__name__)

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

def t1():
    db_config = 'postgresql+psycopg2://admin:admin_password@db:5432/meu_banco'
    c = CalculadoraIndicadoresFinanceiros(db_config, 7617)
    df_resultados = c.executar()
    df_resultados = df_resultados.dropna(subset=df_resultados.columns.difference(['data_fim', 'id_empresa']), how='all')
    print(df_resultados.head())
    df_resultados = c.calculo_indicadores(df_resultados)
    print(df_resultados.head())
    _save_results_to_db(df_resultados)

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
    'dag_teste',
    default_args=default_args,
    description='DAG de teste',
    schedule_interval='@daily',
    tags=['b3', 'reports', 'financial_data'],
)

t = PythonOperator(
    task_id='t1',
    python_callable=t1,
    dag=dag,
)

t

