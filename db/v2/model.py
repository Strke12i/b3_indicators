from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import DDL, event

Base = declarative_base()

class Empresa(Base):
    __tablename__ = 'empresa'
    id_empresa = Column(Integer, primary_key=True)
    nome_empresa = Column(String)
    ticker = Column(String, nullable=True)
    data_ipo = Column(DateTime, nullable=True)
    url_imagem = Column(String, nullable=True)

class Ticker(Base):
    __tablename__ = 'ticker'
    codigo_isin = Column(String, primary_key = True)
    id_empresa = Column(Integer, ForeignKey('empresa.id_empresa'))
    ticker = Column(String, nullable=False)
    
    empresa = relationship('Empresa', backref='tickers')

class Cotacao(Base):
    __tablename__ = 'cotacao'
    codigo_isin = Column(String, ForeignKey('ticker.codigo_isin'), primary_key=True)
    data_pregao = Column(DateTime, primary_key=True)
    abertura = Column(Float)
    fechamento = Column(Float)
    numero_de_negocios = Column(Float)
    quantidade_negociada = Column(Float)
    volume_negociado = Column(Float)
    
    ticker = relationship('Ticker', back_populates='cotacoes')
    
    __table_args__ = (PrimaryKeyConstraint('codigo_isin', 'data_pregao'), {})

class Relatorio(Base):
    __tablename__ = 'relatorio'
    id_relatorio = Column(Integer, primary_key=True)
    id_empresa = Column(Integer, ForeignKey('empresa.id_empresa'))
    data_inicio = Column(DateTime, nullable=True)
    data_fim = Column(DateTime, nullable=False)
    tipo_relatorio = Column(String)
    ultima_atualizacao = Column(DateTime, default=datetime.now)

    empresa = relationship('Empresa')

class DadosRelatorio(Base):
    __tablename__ = 'dados_relatorio'
    codigo_conta = Column(String, primary_key=True)
    id_relatorio = Column(Integer, ForeignKey('relatorio.id_relatorio', ondelete='CASCADE'), primary_key=True)
    descricao = Column(String)
    valor = Column(Float)

    __table_args__ = (PrimaryKeyConstraint('codigo_conta', 'id_relatorio'), {})

    relatorio = relationship('Relatorio')

class Indicadores(Base):
    __tablename__ = 'indicadores'
    # Chave primeria composta por id_empresa, data_fim, tempo_analisado
    id_empresa = Column(Integer, ForeignKey('empresa.id_empresa'), nullable=False, primary_key=True)
    data_fim = Column(DateTime, primary_key=True)     # Mapeia para TIMESTAMP no PostgreSQL
    tempo_analisado = Column(Integer, primary_key=True)

    __table_args__ = (PrimaryKeyConstraint('id_empresa', 'data_fim', 'tempo_analisado'), {})

    receita_liquida = Column(Float)
    ebit = Column(Float)
    resultado_liquido = Column(Float)
    participacao_nao_controladora = Column(Float)
    deprec = Column(Float)
    ativo_total = Column(Float)
    lucro_liquido = Column(Float)
    passivo_circulante = Column(Float)
    total_emprestimos_e_financiamentos = Column(Float)
    caixa_e_equivalentes= Column(Float)
    aplicacoes_financeiras = Column(Float)
    patrimonio_liquido = Column(Float)
    participacao_nao_controladora_acionistas = Column(Float)
    total_emprestimos_e_financiamentos_lp = Column(Float)
    capital_investido = Column(Float)
    ebitda = Column(Float)
    divida_bruta = Column(Float)
    roic = Column(Float)
    roe = Column(Float)
    roa = Column(Float)
    divida_liquida = Column(Float)
    margem_liquida = Column(Float)
    margem_ebit = Column(Float)
    divida_liquida_ebitda = Column(Float)
    ultima_atualizacao = Column(DateTime, default=datetime.now)

    empresa = relationship('Empresa')


engine = create_engine('postgresql+psycopg2://admin:admin_password@localhost:5433/meu_banco')
Base.metadata.create_all(engine)

event.listen(
    Relatorio.__table__,
    'after_create',
    DDL("""
        ALTER TABLE %(table) ADD CONSTRAINT %(constraint_name)
        UNIQUE NULLS NOT DISTINCT (tipo_relatorio, id_empresa, data_inicio, data_fim)
    """).execute_if(dialect='postgresql'),
)
