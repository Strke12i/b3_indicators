from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime, PrimaryKeyConstraint
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime

Base = declarative_base()

class Empresa(Base):
    __tablename__ = 'empresa'
    id_empresa = Column(Integer, primary_key=True)
    nome_empresa = Column(String, unique=True)
    ticker = Column(String, nullable=True)
    data_ipo = Column(DateTime, nullable=True)
    url_imagem = Column(String, nullable=True)

class Dividendos(Base):
    __tablename__ = 'dividendos'
    id_dividendo = Column(Integer, primary_key=True)
    nome_empresa = Column(String, ForeignKey('empresa.nome_empresa'), nullable=False)
    data_aprovacao = Column(DateTime, nullable=False)

    tipo_ativo = Column(String, nullable=False)  # Ex: 'ON'
    valor_provento = Column(Float)
    proventos_por_unidade = Column(Integer)  # 1 = por unidade, 1000 = por mil
    tipo_provento = Column(String)  # Ex: 'DIVIDENDO', 'JRS CAP PROPRIO'
    ultimo_dia_com = Column(DateTime, )
    data_ultimo_preco_com = Column(DateTime)
    ultimo_preco_com = Column(Float)
    preco_por_unidade = Column(Integer)  # Sempre 1 neste caso
    provento_preco_percentual = Column(Float)  # % do provento em relação ao preço

    empresa = relationship('Empresa')

class Ticker(Base):
    __tablename__ = 'ticker'
    ticker = Column(String, primary_key=True)
    empresa_nome = Column(String, ForeignKey('empresa.nome_empresa'))

    empresa = relationship('Empresa')

class Cotacao(Base):
    __tablename__ = 'cotacao'
    ticker = Column(String, ForeignKey('ticker.ticker'), primary_key=True)
    data = Column(DateTime, primary_key=True)
    abertura = Column(Float)
    fechamento = Column(Float)
    __table_args__ = (PrimaryKeyConstraint('ticker', 'data'), {})

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
    id_dado = Column(Integer, primary_key=True)
    id_relatorio = Column(Integer, ForeignKey('relatorio.id_relatorio'))
    descricao = Column(String)
    valor = Column(Float)

    relatorio = relationship('Relatorio')

class UrlsVisitadas(Base):  
    __tablename__ = 'urls_visitadas'
    id_empresa = Column(Integer, ForeignKey('empresa.id_empresa'), primary_key=True)
    url = Column(String, primary_key=True)
    ultima_atualizacao = Column(DateTime, default=datetime.now)

    __tables_args__ = (PrimaryKeyConstraint('id_empresa', 'url'), {})

    empresa = relationship('Empresa')


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
