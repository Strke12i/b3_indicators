import pandas as pd
import numpy as np

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
                'descricao': "Receita de Venda de Bens e/ou Serviços"
            },
            'ebit': {
                'tipo_relatorio': "Demonstração do Resultado",
                'descricao': "Resultado Antes do Resultado Financeiro e dos Tributos"
            },
            'resultado_liquido': {
                'tipo_relatorio': "Demonstração do Resultado",
                'descricao': "Resultado Líquido das Operações Continuadas"
            },
            'participacao_nao_controladora': {
                'tipo_relatorio': "Demonstração do Resultado",
                'descricao': "Atribuído a Sócios Não Controladores"
            },
            'lucro_liquido': {
                'tipo_relatorio': "Demonstração do Resultado",
                'descricao': "Atribuído a Sócios da Empresa Controladora"
            },
            'deprec': {
                'tipo_relatorio': "Demonstração do Fluxo de Caixa",
                'descricoes': [
                    "Depreciação, Amortização e Exaustão",
                    "Depreciação e amortização",
                    "Depreciação, depleção e amortização"
                ]
            }
        }
        self.metricas_ajustadas = {
            'ativo_total': {
                'tipo_relatorio': "Balanço Patrimonial Ativo",
                'descricao': "Ativo Total"
            },
            'passivo_circulante': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'descricao': "Passivo Circulante"
            },
            'total_emprestimos_e_financiamentos': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'descricao': "Empréstimos e Financiamentos"
            },
            'caixa_e_equivalentes': {
                'tipo_relatorio': "Balanço Patrimonial Ativo",
                'descricao': "Caixa e Equivalentes de Caixa"
            },
            'aplicacoes_financeiras': {
                'tipo_relatorio': "Balanço Patrimonial Ativo",
                'descricoes': ["Aplicações Financeiras1", "Aplicações Financeiras"]
            },
            'patrimonio_liquido': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'descricao': "Patrimônio Líquido Consolidado"
            },
            'participacao_nao_controladora_acionistas': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'descricao': "Participação dos Acionistas Não Controladores"
            },
            'total_emprestimos_e_financiamentos_lp': {
                'tipo_relatorio': "Balanço Patrimonial Passivo",
                'descricao': "Empréstimos e Financiamentos3"
            },
        }

    def carregar_dados(self):
        df_dados = pd.read_sql_table('dados_relatorio', self.db_config)
        df_relatorio = pd.read_sql_table('relatorio', self.db_config)

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

    def adicionar_metrica(self, nome_metrica, tipo_relatorio, descricao, ajustada=False):
        if ajustada:
            self.metricas_ajustadas[nome_metrica] = {
                'tipo_relatorio': tipo_relatorio,
                'descricao': descricao
            }
        else:
            self.metricas[nome_metrica] = {
                'tipo_relatorio': tipo_relatorio,
                'descricao': descricao
            }

    def filtrar_metricas(self):
        if self.df is None:
            raise ValueError("Dados não carregados. Execute carregar_dados() primeiro.")
        
        for nome_metrica, config in self.metricas.items():
            if nome_metrica == 'deprec':
                # Tratamento especial para deprec com múltiplas descrições
                df_metrica = pd.DataFrame()
                for descricao in config['descricoes']:
                    filtro = self._filtrar_metrica(
                        self.df, config['tipo_relatorio'], descricao, nome_metrica
                    )
                    if not filtro.empty:
                        # Usar a primeira descrição encontrada com dados
                        df_metrica = filtro
                        break
                self.dataframes_metricas[nome_metrica] = df_metrica
            else:
                self.dataframes_metricas[nome_metrica] = self._filtrar_metrica(
                    self.df, config['tipo_relatorio'], config['descricao'], nome_metrica
                )
        
        for nome_metrica, config in self.metricas_ajustadas.items():
            if nome_metrica == 'aplicacoes_financeiras':
                # Tratamento especial para aplicacoes_financeiras com múltiplas descrições
                df_ajustado = pd.DataFrame()
                for descricao in config['descricoes']:
                    filtro = self._filtrar_metrica(
                        self.df, config['tipo_relatorio'], descricao, nome_metrica
                    )
                    if not filtro.empty:
                        # Usar a primeira descrição encontrada com dados
                        df_ajustado = filtro
                        break
                self.dataframes_ajustados[nome_metrica] = df_ajustado[
                    df_ajustado['data_inicio'].isna()
                ][['id_empresa', 'data_fim', nome_metrica]]
            else:
                self.dataframes_ajustados[nome_metrica] = self._filtrar_metrica(
                    self.df, config['tipo_relatorio'], config['descricao'], nome_metrica
                )

    def _filtrar_metrica(self, dados, tipo_relatorio, descricao, nome_coluna_valor):
        filtro = dados[
            (dados["tipo_relatorio"] == tipo_relatorio) &
            (dados["descricao"] == descricao)
        ]
        return filtro.rename(columns={"valor": nome_coluna_valor})

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
            # Adicionar mais um ano ao data_fim_ano_anterior
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
        }

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
            'margem_liquida': (df_resultados['lucro_liquido'] + df_resultados['participacao_nao_controladora']) / df_resultados['receita_liquida'],
    
        }
        
        for nome, serie in indicadores_secundarios.items():
            df_resultados[nome] = serie

        df_resultados['divida_liquida_ebitda'] = df_resultados['divida_liquida'] / df_resultados['ebitda']
        
        # Formatar todos os valores para 2 casas decimais
        for coluna in df_resultados.columns:
            if coluna not in ['id_empresa', 'data_fim']:
                df_resultados[coluna] = df_resultados[coluna].round(4)
        
        return df_resultados