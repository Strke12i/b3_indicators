from sqlalchemy.orm import Session
from model import Empresa, Relatorio, DadosRelatorio
from datetime import datetime
import pandas as pd

def insert_data(session: Session, data: dict, empresa: str, code_cvm: int):
    # Transforma os dados em um formato que pode ser inserido no banco de dados
    if not session.query(Empresa).filter(Empresa.nome_empresa == empresa).first():
        session.add(Empresa(nome_empresa=empresa, id_empresa=code_cvm))
        session.commit()

    empresa_id = session.query(Empresa).filter(Empresa.nome_empresa == empresa).first().id_empresa
    print(f"Empresa ID: {empresa_id}")

    for key, value in data.items():
        # transformar o value em um dataframe
        value = pd.DataFrame(value)
        for data in value.columns:
            original_data = data
            data = data.replace('\xa0', ' ')
            data_inicio = None
            if key not in ['Balanço Patrimonial Ativo', 'Balanço Patrimonial Passivo']:
                data_inicio = data.split('  a  ')[0]
                data_inicio = datetime.strptime(data_inicio, '%d/%m/%Y')
                data_inicio = datetime(data_inicio.year, data_inicio.month, data_inicio.day)


            data_fim = data.split('  a  ')[1]
            data_fim = datetime.strptime(data_fim, '%d/%m/%Y')
            data_fim = datetime(data_fim.year, data_fim.month, data_fim.day)
            if not session.query(Relatorio).filter(Relatorio.id_empresa == empresa_id, Relatorio.data_inicio == data_inicio, Relatorio.data_fim == data_fim, Relatorio.tipo_relatorio == key).first():
                session.add(Relatorio(id_empresa=empresa_id, data_inicio=data_inicio, data_fim=data_fim, tipo_relatorio=key))
                session.commit()

            relatorio_id = session.query(Relatorio).filter(Relatorio.id_empresa == empresa_id, Relatorio.data_inicio == data_inicio, Relatorio.data_fim == data_fim, Relatorio.tipo_relatorio == key).first().id_relatorio
            for index, row in value.iterrows():
                # Converta tipos numpy para tipos nativos
                descricao = str(index)
                valor = float(row[original_data])
                if isinstance(valor, (int, float, str)):
                    valor = valor  # Já é um tipo nativo
                else:
                    # converter de numpy para nativo
                    valor = 0.0

                if not session.query(DadosRelatorio).filter(DadosRelatorio.id_relatorio == relatorio_id, DadosRelatorio.descricao == descricao).first():
                    session.add(DadosRelatorio(id_relatorio=relatorio_id, descricao=descricao, valor=valor))
                    session.commit()
                else:
                    session.query(DadosRelatorio).filter(DadosRelatorio.id_relatorio == relatorio_id, DadosRelatorio.descricao == descricao).update({'valor': valor})
                    session.commit()

    session.close()
