import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import gc

class CalculadoraIndicadoresFinanceiros:
    def __init__(self, db_config, id_empresa):
        self.db_config = db_config
        self.id_empresa = id_empresa
        self.df = None
        self.dataframes_metricas = {}
        self.dataframes_ajustados = {}
        self.metricas = {
            'receita_liquida': {
                'tipo_relatorio': "Demonstração do Resultado",
                'codigo_conta': 301,
                'descricao': ["Receita de Venda de Bens e/ou Serviços", "Receitas de Intermediação Financeira"]
            },
            'ebit': {
                'tipo_relatorio': "Demonstração do Resultado",
                'codigo_conta': 305,
                'descricao': ["Resultado Antes do Resultado Financeiro e dos Tributos"]
            },
            'resultado_liquido': {
                'tipo_relatorio': "Demonstração do Resultado",
                'codigo_conta': 309,
                'descricao': ["Resultado Líquido das Operações Continuadas"]
            },
            'participacao_nao_controladora': {
                'tipo_relatorio': "Demonstração do Resultado",
                'codigo_conta': 31102,
                'descricao': ["Atribuído a Sócios Não Controladores"]
            },
            'lucro_periodo': {
                'tipo_relatorio': "Demonstração do Resultado",
                'codigo_conta': 311,
                'descricao': ["Lucro/Prejuízo Consolidado do Período"]
            },
            'socios_nao_participadores': {
                'tipo_relatorio': "Demonstração do Resultado",
                'codigo_conta': 31102,
                'descricao': ["Atribuído a Sócios Não Controladores"]
            },
            'deprec': {
                'tipo_relatorio': "Demonstração do Fluxo de Caixa",
                'codigo_conta': [6010102, 6010103, 6010108],
                'descricao': ["Depreciação, Amortização e Exaustão", "Depreciação e amortização", "Depreciação"]
            }
        }
        self.metricas_ajustadas = {
            'ativo_total': {
                'tipo_relatorio': "Balanço Patrimonial Ativo",
                'codigo_conta': 1,
                'descricao': ["Ativo Total"]
            },
            'passivo_circulante': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'codigo_conta': 201,
                'descricao': ["Passivo Circulante"]
            },
            'total_emprestimos_e_financiamentos': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'codigo_conta': [20104, 201041],
                'descricao': ["Empréstimos e Financiamentos", "Financiamentos e Empréstimos"]
            },
            'caixa_e_equivalentes': {
                'tipo_relatorio': "Balanço Patrimonial Ativo",
                'codigo_conta': 10101,
                'descricao': ["Caixa e Equivalentes de Caixa"]
            },
            'aplicacoes_financeiras': {
                'tipo_relatorio': "Balanço Patrimonial Ativo",
                'codigo_conta': 10102,
                'descricao': ["Aplicações Financeiras"]
            },
            'patrimonio_liquido': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'codigo_conta': [203, 207],
                'descricao': ["Patrimônio Líquido Consolidado"]
            },
            'participacao_nao_controladora_acionistas': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'codigo_conta': 20309,
                'descricao': ["Participação dos Acionistas Não Controladores"]
            },
            'total_emprestimos_e_financiamentos_lp': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'codigo_conta': [20201, 2020101, 2010108, 202010106, 201040201],
                'descricao': ["Empréstimos e Financiamentos"]
            },
        }

    def carregar_dados(self):
        df_relatorio = pd.read_sql("SELECT * FROM relatorio WHERE id_empresa = %s", self.db_config, params=(self.id_empresa,))

        ids_relatorio = df_relatorio['id_relatorio'].unique()
        ids_relatorio = [int(id_relatorio) for id_relatorio in ids_relatorio]
        placeholder = ', '.join(['%s'] * len(ids_relatorio))
        query = f"SELECT * FROM dados_relatorio WHERE id_relatorio IN ({placeholder})"
        df_dados = pd.read_sql(query, self.db_config, params=tuple(ids_relatorio))

        if 'id_empresa' not in df_dados.columns:
            if 'id_empresa' in df_relatorio.columns:
                self.df = pd.merge(df_dados, df_relatorio, on='id_relatorio')
                self.df = self.df[self.df['id_empresa'] == self.id_empresa]
            else:
                raise KeyError("A coluna 'id_empresa' não foi encontrada em 'dados_relatorio' nem em 'relatorio'.")
        else:
            df_dados = df_dados[df_dados['id_empresa'] == self.id_empresa]
            self.df = pd.merge(df_dados, df_relatorio, on='id_relatorio')

        self.df['data_inicio'] = pd.to_datetime(self.df['data_inicio'])
        self.df['data_fim'] = pd.to_datetime(self.df['data_fim'])


    def adicionar_metrica(self, nome_metrica, tipo_relatorio, codigo_conta, descricao, ajustada=False):
        # Garantir que descricao seja uma lista, mesmo que tenha um único item
        if not isinstance(descricao, (list, tuple)):
            descricao = [descricao]
        if ajustada:
            self.metricas_ajustadas[nome_metrica] = {
                'tipo_relatorio': tipo_relatorio,
                'codigo_conta': codigo_conta,
                'descricao': descricao
            }
        else:
            self.metricas[nome_metrica] = {
                'tipo_relatorio': tipo_relatorio,
                'codigo_conta': codigo_conta,
                'descricao': descricao
            }

    def _filtrar_metrica(self, dados, tipo_relatorio, codigo_conta, descricao, nome_coluna_valor, limiar_similaridade=99):
        # Função auxiliar para verificar similaridade
        def is_similar(desc_df, desc_ref_list):
            if pd.isna(desc_df):
                return False
            return any(fuzz.token_sort_ratio(desc_df, desc_ref) >= limiar_similaridade for desc_ref in desc_ref_list)

        # Filtrar por tipo_relatorio e codigo_conta primeiro
        if isinstance(codigo_conta, (list, tuple)):
            dados_filtrados = dados[
                (dados["tipo_relatorio"] == tipo_relatorio) &
                (dados["codigo_conta"].isin(codigo_conta))
            ]
        else:
            dados_filtrados = dados[
                (dados["tipo_relatorio"] == tipo_relatorio) &
                (dados["codigo_conta"] == codigo_conta)
            ]

        # Verificar similaridade com qualquer uma das descrições fornecidas
        filtro = dados_filtrados[
            dados_filtrados["descricao"].apply(lambda x: is_similar(x, descricao))
        ]

        return filtro.rename(columns={"valor": nome_coluna_valor})

    def filtrar_metricas(self):
        if self.df is None:
            raise ValueError("Dados não carregados. Execute carregar_dados() primeiro.")
        
        datas_fim = self.df['data_fim'].unique()
        
        for nome_metrica, config in self.metricas.items():
            df_metrica = pd.DataFrame()
            if isinstance(config['codigo_conta'], (list, tuple)):
                # Para múltiplos códigos, tentar cada um até encontrar um match
                for codigo in config['codigo_conta']:
                    filtro = self._filtrar_metrica(
                        self.df, config['tipo_relatorio'], codigo, config['descricao'], nome_metrica
                    )
                    if not filtro.empty:
                        df_metrica = filtro
                        break
            else:
                df_metrica = self._filtrar_metrica(
                    self.df, config['tipo_relatorio'], config['codigo_conta'], config['descricao'], nome_metrica
                )
            
            if df_metrica.empty:
                df_metrica = pd.DataFrame({
                    'id_empresa': [self.id_empresa] * len(datas_fim),
                    'data_fim': datas_fim,
                    'data_inicio': [pd.NaT] * len(datas_fim),
                    nome_metrica: [0] * len(datas_fim)
                })
            self.dataframes_metricas[nome_metrica] = df_metrica
            del df_metrica
            gc.collect()
        
        for nome_metrica, config in self.metricas_ajustadas.items():
            df_ajustado = pd.DataFrame()
            if isinstance(config['codigo_conta'], (list, tuple)):
                # Para múltiplos códigos, tentar cada um até encontrar um match
                for codigo in config['codigo_conta']:
                    filtro = self._filtrar_metrica(
                        self.df, config['tipo_relatorio'], codigo, config['descricao'], nome_metrica
                    )
                    if not filtro.empty:
                        df_ajustado = filtro
                        break
            else:
                df_ajustado = self._filtrar_metrica(
                    self.df, config['tipo_relatorio'], config['codigo_conta'], config['descricao'], nome_metrica
                )
            
            if df_ajustado.empty:
                df_ajustado = pd.DataFrame({
                    'id_empresa': [self.id_empresa] * len(datas_fim),
                    'data_fim': datas_fim,
                    'data_inicio': [pd.NaT] * len(datas_fim),
                    nome_metrica: [0] * len(datas_fim)
                })
            if not df_ajustado.empty and 'data_inicio' in df_ajustado.columns:
                df_ajustado = df_ajustado[df_ajustado['data_inicio'].isna()]
            self.dataframes_ajustados[nome_metrica] = df_ajustado[['id_empresa', 'data_fim', nome_metrica]]
            del df_ajustado
            gc.collect()

    def calcular_valor_12m(self, df, data_fim_target, valor_col):
        data_fim_target = pd.to_datetime(data_fim_target)
        ano = data_fim_target.year
        mes = data_fim_target.month
        dia = data_fim_target.day
        ano_anterior = ano - 1

        try:
            data_fim_ano_anterior = pd.to_datetime(f'{ano_anterior}-{mes:02d}-{dia:02d}')
        except ValueError:
            ultimo_dia = pd.Timestamp(ano_anterior, mes, 1) + pd.offsets.MonthEnd(0)
            data_fim_ano_anterior = ultimo_dia

        data_inicio_ano_anterior = pd.to_datetime(f'{ano_anterior}-01-01')
        data_fim_ano_anterior_completo = pd.to_datetime(f'{ano_anterior}-12-31')
        if 'data_inicio' not in df.columns:
            return None
        linha_ano_anterior = df[
            (df['data_inicio'] == data_inicio_ano_anterior) & 
            (df['data_fim'] == data_fim_ano_anterior_completo)
        ]
        if linha_ano_anterior.empty:
            data_inicio_ano_anterior = pd.to_datetime(f'{ano_anterior}-04-01')
            data_fim_ano_anterior_completo = pd.to_datetime(f'{ano}-03-31')
            if 'data_inicio' not in df.columns:
                return None
            linha_ano_anterior = df[
                (df['data_inicio'] == data_inicio_ano_anterior) & 
                (df['data_fim'] == data_fim_ano_anterior_completo)
            ]
            if linha_ano_anterior.empty:
                return None
        
        valor_acumulado_ano_anterior = linha_ano_anterior[valor_col].values[0]

        linha_ate_data_fim_ano_anterior = df[
            (df['data_inicio'] == data_inicio_ano_anterior) & 
            (df['data_fim'] == data_fim_ano_anterior)
        ]
        if linha_ate_data_fim_ano_anterior.empty:
            data_fim_ano_anterior = pd.to_datetime(f'{ano_anterior+1}-{mes:02d}-{dia:02d}')
            linha_ate_data_fim_ano_anterior = df[
                (df['data_inicio'] == data_inicio_ano_anterior) & 
                (df['data_fim'] == data_fim_ano_anterior)
            ]
            if linha_ate_data_fim_ano_anterior.empty:
                return None
        
        valor_acumulado_ate_data_fim_ano_anterior = linha_ate_data_fim_ano_anterior[valor_col].values[0]

        valor_ultimos_meses_ano_anterior = valor_acumulado_ano_anterior - valor_acumulado_ate_data_fim_ano_anterior

        data_inicio_ano = pd.to_datetime(f'{ano}-01-01')
        linha_ate_data_fim_target = df[
            (df['data_inicio'] == data_inicio_ano) & 
            (df['data_fim'] == data_fim_target)
        ]
        if linha_ate_data_fim_target.empty:
            data_inicio_ano = pd.to_datetime(f'{ano-1}-04-01')
            if mes > 4:
                data_inicio_ano = pd.to_datetime(f'{ano}-04-01')
            linha_ate_data_fim_target = df[
                (df['data_inicio'] == data_inicio_ano) & 
                (df['data_fim'] == data_fim_target)
            ]
            if linha_ate_data_fim_target.empty:
                return None

        valor_acumulado_ate_data_fim_target = linha_ate_data_fim_target[valor_col].values[0]

        return valor_ultimos_meses_ano_anterior + valor_acumulado_ate_data_fim_target

    def calcular_indicadores_12m(self):
        if not self.dataframes_metricas:
            raise ValueError("Métricas não filtradas. Execute filtrar_metricas() primeiro.")
        
        datas_fim = self.dataframes_metricas['receita_liquida']['data_fim'].unique()
        resultado_12m = {nome: [] for nome in self.metricas.keys()}
        resultado_12m['data_fim'] = []

        for data_fim in datas_fim:
            valores = {}
            for nome_metrica, df_metrica in self.dataframes_metricas.items():
                valor_12m = self.calcular_valor_12m(df_metrica, data_fim, nome_metrica)
                
                valores[nome_metrica] = valor_12m if valor_12m is not None else np.nan
            
            resultado_12m['data_fim'].append(data_fim)
            for nome_metrica in self.metricas.keys():
                resultado_12m[nome_metrica].append(valores[nome_metrica])

        df_resultado = pd.DataFrame(resultado_12m)
        df_resultado['id_empresa'] = self.id_empresa
        df_resultado['data_fim'] = pd.to_datetime(df_resultado['data_fim'])

        for nome_metrica, df_ajustado in self.dataframes_ajustados.items():
            df_ajustado = df_ajustado[['id_empresa', 'data_fim', nome_metrica]].drop_duplicates(
                subset=['id_empresa', 'data_fim'], keep='last'
            )
            df_resultado = df_resultado.merge(
                df_ajustado[['id_empresa', 'data_fim', nome_metrica]],
                on=['id_empresa', 'data_fim'],
                how='left'
            )

        df_resultado = df_resultado.sort_values('data_fim')
        return df_resultado

    def executar(self):
        self.carregar_dados()
        self.filtrar_metricas()
        return self.calcular_indicadores_12m()

    def calculo_indicadores(self, df_resultados):
        df_resultados.fillna(0, inplace=True)
        indicadores_primarios = {
            'capital_investido': (
                df_resultados['ativo_total'] - 
                df_resultados['passivo_circulante'] + 
                df_resultados['total_emprestimos_e_financiamentos'] - 
                df_resultados['caixa_e_equivalentes'] - 
                df_resultados['aplicacoes_financeiras']
            ),
            'ebitda': df_resultados['ebit'] + df_resultados['deprec'],
            'divida_bruta': (
                df_resultados['total_emprestimos_e_financiamentos'] + 
                df_resultados['total_emprestimos_e_financiamentos_lp']
            ),
            'patrimonio_liquido': (
                df_resultados['patrimonio_liquido'] - 
                df_resultados['participacao_nao_controladora_acionistas']
            ),
            'lucro_liquido': (
                df_resultados['lucro_periodo'] -
                df_resultados['socios_nao_participadores']
            ),
        }
        
        df_resultados.drop(columns=['lucro_periodo','socios_nao_participadores'], inplace = True)

        for nome, serie in indicadores_primarios.items():
            df_resultados[nome] = serie

        indicadores_secundarios = {
            'roic': (df_resultados['ebit'] * 0.66) / df_resultados['capital_investido'],
            'roe': (
                (df_resultados['lucro_liquido'] + df_resultados['participacao_nao_controladora']) / 
                (df_resultados['patrimonio_liquido'] + df_resultados['participacao_nao_controladora_acionistas'])
            ),
            'roa': (
                (df_resultados['lucro_liquido'] + df_resultados['participacao_nao_controladora']) / 
                df_resultados['ativo_total']
            ),
            'divida_liquida': (
                df_resultados['divida_bruta'] - 
                df_resultados['caixa_e_equivalentes'] - 
                df_resultados['aplicacoes_financeiras']
            ),
            'margem_ebit': df_resultados['ebit'] / df_resultados['receita_liquida'],
            'margem_liquida': (
                df_resultados['lucro_liquido'] + df_resultados['participacao_nao_controladora']
            ) / df_resultados['receita_liquida'],
        }
        
        for nome, serie in indicadores_secundarios.items():
            df_resultados[nome] = serie

        df_resultados['divida_liquida_ebitda'] = df_resultados['divida_liquida'] / df_resultados['ebitda']
        
        for coluna in df_resultados.columns:
            if coluna not in ['id_empresa', 'data_fim']:
                df_resultados[coluna] = df_resultados[coluna].round(4)
        
        return df_resultados
    
    def _save_results_to_db(self, df_resultado, session, indicators_table):
        from sqlalchemy.dialects.postgresql import insert

        try:
            # Converter DataFrame para lista de dicionários
            df_resultado['tempo_analisado'] = 12
            records = df_resultado.to_dict('records')

            # Inserção/Atualização em lote
            for record in records:
                stmt = insert(indicators_table).values(record)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['id_empresa', 'data_fim', 'tempo_analisado'],
                    set_=record
                )
                session.execute(stmt)
            session.commit()
            print(f"Salvo {len(records)} registros para empresa {self.id_empresa}")
        except Exception as e:
            print(f"Erro ao salvar no banco: {e}")
            session.rollback()
            raise
    
    def run(self, session, indicators_table):
        df_resultado = self.executar()
        df_resultado = df_resultado.dropna(subset=df_resultado.columns.difference(['data_fim', 'id_empresa']), how='all')
        df_resultado = self.calculo_indicadores(df_resultado)
        df_resultado.drop_duplicates(subset=['data_fim'], keep='last', inplace=True)
        
        self._save_results_to_db(df_resultado, session, indicators_table)
            