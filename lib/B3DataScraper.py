import pandas as pd
from bs4 import BeautifulSoup, SoupStrainer
import base64
import os
import cloudscraper
import logging
import concurrent.futures
from typing import Dict, List, Optional, Union, Tuple, Any
from dataclasses import dataclass
import json
from functools import lru_cache
from B3DataFetcher import B3DataFetcher

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('b3_extractor')

# Constantes
STRUCTURED_REPORTS_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListStructuredReports/"
CVM_BASE_URL = "https://www.rad.cvm.gov.br/ENET/"

# Tipos de relatórios padrão para extração
DEFAULT_REPORT_TYPES = [
    "Balanço Patrimonial Ativo",
    "Balanço Patrimonial Passivo",
    "Demonstração do Resultado",
    "Demonstração do Resultado Abrangente",
    "Demonstração do Fluxo de Caixa",
    "Demonstração de Valor Adicionado",
]

@dataclass
class CompanyInfo:
    """Classe para armazenar informações da empresa"""
    code_cvm: int
    name: str
    
    def __post_init__(self):
        self.code_cvm = int(self.code_cvm)

class ReportNotFoundException(Exception):
    """Exceção para quando um relatório não é encontrado"""
    pass

class B3Extractor:
    """
    Classe para extrair dados financeiros de empresas listadas na B3
    """
    
    def __init__(self, df_companies: pd.DataFrame, max_workers: int = 4, cache_size: int = 128):
        """
        Inicializa o extrator B3
        
        Args:
            df_companies: DataFrame com informações das empresas
            max_workers: Número máximo de threads para processamento paralelo
            cache_size: Tamanho do cache LRU para requisições
        """
        self.scraper = cloudscraper.create_scraper()
        self.max_workers = max_workers

        self.df_companies = df_companies
        logger.info(f"Carregadas informações de {len(self.df_companies)} empresas")

        # Aplicar cache às funções de requisição
        self._get = lru_cache(maxsize=cache_size)(self._get)
        
    def _get(self, url: str) -> bytes:
        """
        Faz uma requisição GET com cache
        
        Args:
            url: URL para a requisição
            
        Returns:
            Conteúdo da resposta
        """
        try:
            response = self.scraper.get(url)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logger.error(f"Erro na requisição para {url}: {e}")
            raise
    
    def get_company_info(self, company_name: str) -> CompanyInfo:
        """
        Obtém informações da empresa pelo nome
        
        Args:
            company_name: Nome da empresa na B3
            
        Returns:
            Objeto CompanyInfo com código CVM
            
        Raises:
            ValueError: Se a empresa não for encontrada
        """
        if self.df_companies is None:
            raise ValueError("CSV de empresas não carregado. Forneça o caminho do CSV ao inicializar.")
            
        company_info = self.df_companies[self.df_companies["issuingCompany"] == company_name]
        if company_info.empty:
            raise ValueError(f"Empresa '{company_name}' não encontrada.")
            
        return CompanyInfo(
            code_cvm=company_info.iloc[0]['codeCVM'],
            name=company_name
        )
    
    def get_structured_reports(self, code_cvm: int, year: int) -> Dict:
        """
        Obtém relatórios estruturados para uma empresa e ano específicos
        
        Args:
            code_cvm: Código CVM da empresa
            year: Ano dos relatórios
            
        Returns:
            Dicionário com os relatórios disponíveis
            
        Raises:
            Exception: Se houver erro na requisição
        """
        payload = json.dumps({"codeCVM": code_cvm, "language": "pt-br", "status": True, "year": year})
        encoded_payload = base64.b64encode(payload.encode()).decode()
        url = STRUCTURED_REPORTS_URL + encoded_payload
        
        response = self.scraper.get(url)
        if response.status_code != 200:
            raise Exception(f"Erro na requisição. Status code: {response.status_code}")
            
        return response.json()
    
    def extract_url_search(self, data_json: Dict) -> str:
        """
        Extrai a URL de busca do DFP do JSON de relatórios
        
        Args:
            data_json: JSON com os relatórios estruturados
            
        Returns:
            URL de busca do relatório
        """
        try:
            return data_json["dfp"][0]["urlSearch"]
        except (KeyError, IndexError) as e:
            logger.error(f"Erro ao extrair URL: {e}")
            return None
    
    def extract_links_from_select(self, url_report: str) -> Dict[str, str]:
        """
        Extrai links de todos os relatórios disponíveis
        
        Args:
            url_report: URL do relatório principal
            
        Returns:
            Dicionário com título do relatório e conteúdo codificado em base64
        """
        if not url_report:
            return None
            
        try:
            # Obter a página com os seletores
            response_content = self._get(url_report)
            soup = BeautifulSoup(response_content, "html.parser")
            
            # Encontrar o seletor de relatórios
            select = soup.find("select", {"id": "cmbQuadro"})
            if not select:
                logger.error("Seletor de relatórios não encontrado")
                return None
                
            options = select.find_all("option")
            
            # Extrair o ID do link do script
            script = soup.find_all("script")[-1]
            link_id = None
            
            for line in script.string.split("\n"):
                if "location=" in line:
                    link_id = line.split("location=")[1].split("'")[1].split("Versao=")[1]
                    break
                    
            if not link_id:
                logger.error("ID do link não encontrado no script")
                return None
            
            # Construir os links para cada opção
            links = {
                option.text: f"{CVM_BASE_URL}{option['value'].replace(' ', '%20')}{link_id}"
                for option in options
            }
            
            # Extrair o conteúdo de cada link em paralelo
            responses = {}
            
            def fetch_link(title_link):
                title, link = title_link
                try:
                    content = self._get(link)
                    return title, base64.b64encode(content).decode('utf-8')
                except Exception as e:
                    logger.error(f"Erro ao extrair link '{title}': {e}")
                    return title, None
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                results = executor.map(fetch_link, links.items())
                
            for title, content in results:
                if content:
                    responses[title] = content
            
            return responses
        except Exception as e:
            logger.error(f"Erro ao extrair links: {e}")
            return None
    
    def get_table_data(self, response: str) -> pd.DataFrame:
        """
        Extrai dados de uma tabela a partir do conteúdo codificado em base64
        
        Args:
            response: Conteúdo da página codificado em base64
            
        Returns:
            DataFrame com os dados da tabela
            
        Raises:
            Exception: Se a tabela não for encontrada
        """
        try:
            decoded_content = base64.b64decode(response).decode('utf-8')
            parse_only = SoupStrainer("table", {"id": "ctl00_cphPopUp_tbDados"})
            soup = BeautifulSoup(decoded_content, "html.parser", parse_only=parse_only)
            
            table = soup.find()
            if not table:
                raise Exception("Tabela não encontrada.")
            
            # Extração eficiente com list comprehension
            rows = [[td.text.strip() for td in tr.find_all("td")] for tr in table.find_all("tr")]
            
            if not rows:
                return pd.DataFrame()
                
            return pd.DataFrame(rows[1:], columns=rows[0])
        except Exception as e:
            logger.error(f"Erro ao extrair dados da tabela: {e}")
            raise
    
    def reorganize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Reorganiza o DataFrame para facilitar a análise
        
        Args:
            df: DataFrame com os dados extraídos
            
        Returns:
            DataFrame reorganizado
        """
        if df.empty:
            return df
            
        try:
            # Copiar o dataframe para evitar warnings de modificação
            df = df.copy()
            
            # Configurar o índice e remover colunas desnecessárias
            df.set_index('Descrição', inplace=True)
            if 'Conta' in df.columns:
                df.drop(columns=['Conta'], inplace=True)
            
            # Converter strings para valores numéricos
            numeric_cols = df.columns
            df[numeric_cols] = (df[numeric_cols]
                            .replace({'\.': '', ',': '.'}, regex=True)
                            .apply(pd.to_numeric, errors='coerce')
                            .fillna(0))
            
            # Lidar com índices duplicados
            if df.index.duplicated().any():
                df.index = df.index + df.groupby(level=0).cumcount().astype(str).replace('0', '')
            
            return df
        except Exception as e:
            logger.error(f"Erro ao reorganizar DataFrame: {e}")
            return df
    
    def fetch_report(self, url_report: str, links: Dict[str, str], data_type: str) -> pd.DataFrame:
        """
        Busca e processa um tipo específico de relatório
        
        Args:
            url_report: URL do relatório principal
            links: Dicionário com os links extraídos
            data_type: Tipo de relatório a buscar
            
        Returns:
            DataFrame com os dados processados
            
        Raises:
            ValueError: Se o tipo de relatório não estiver disponível
        """
        if not url_report:
            raise ValueError(f"URL do relatório não fornecida.")
        
        if not links or data_type not in links:
            raise ValueError(f"Tipo de dado '{data_type}' não disponível.")
        
        response = links[data_type]
        df = self.get_table_data(response)
        df = self.reorganize_df(df)
        
        return df
    
    def fetch_all_reports(self, url_report: str, links: Dict[str, str], report_types: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
        """
        Busca e processa todos os tipos de relatórios especificados
        
        Args:
            url_report: URL do relatório principal
            links: Dicionário com os links extraídos
            report_types: Lista de tipos de relatórios a buscar
            
        Returns:
            Dicionário com os relatórios processados
        """
        if report_types is None:
            report_types = DEFAULT_REPORT_TYPES
            
        data = {}
        
        def process_report(data_type):
            try:
                return data_type, self.fetch_report(url_report, links, data_type)
            except Exception as e:
                logger.error(f"Erro ao extrair dados de '{data_type}': {e}")
                return data_type, None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = executor.map(process_report, report_types)
            
        for data_type, df in results:
            data[data_type] = df
            
        return data
    
    def get_all_itr_links(self, data_json: Dict) -> Dict[str, str]:
        """
        Extrai links de todos os relatórios ITR disponíveis
        
        Args:
            data_json: JSON com os relatórios estruturados
            
        Returns:
            Dicionário com data do relatório e URL
        """
        itr_links = {}
        try:
            itr = data_json.get('itr', [])
            for i in itr:
                date = i['dateTimeReference'].split('T')[0]
                itr_links[date] = i['urlSearch']
                
            return itr_links
        except Exception as e:
            logger.error(f"Erro ao extrair links de ITR: {e}")
            return itr_links
    
    def transform_report_dates(self, data: Dict[str, pd.DataFrame], year_ref: int) -> Dict[str, pd.DataFrame]:
        """
        Transforma as datas nos relatórios para um formato consistente
        
        Args:
            data: Dicionário com os relatórios processados
            year_ref: Ano de referência
            
        Returns:
            Dicionário com os relatórios transformados
        """
        for key in ['Balanço Patrimonial Ativo', 'Balanço Patrimonial Passivo']:
            if key not in data or data[key] is None or data[key].empty:
                continue
                
            df = data[key]
            for column in df.columns:
                year = column.split('/')[-1]
                if year != str(year_ref):
                    new_column = f'01/01/{year}  a  {column}'
                    df.rename(columns={column: new_column}, inplace=True)
                else:
                    new_column = f'01/01/{year}  a  {column}'
                    df.rename(columns={column: new_column}, inplace=True)
        
        return data
    
    def fetch_quarterly_data(self, 
                           company_name: str,
                           year: int,
                           report_types: Optional[List[str]] = None) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Busca dados trimestrais (ITR) e anuais (DFP) para uma empresa
        
        Args:
            company_name: Nome da empresa
            year: Ano dos relatórios
            report_types: Lista de tipos de relatórios a buscar
            
        Returns:
            Dicionário com data do relatório e dados processados
        """
        # Obter informações da empresa
        company_info = self.get_company_info(company_name)
        
        # Obter relatórios do ano atual
        data_json_current = self.get_structured_reports(company_info.code_cvm, year)
        
        # Obter relatórios do ano seguinte (para DFP do fim de ano)
        data_json_next = self.get_structured_reports(company_info.code_cvm, year + 1)
        
        # Extrair URL do DFP
        url_report = self.extract_url_search(data_json_next)
        if not url_report:
            raise ReportNotFoundException(f"Não foi possível localizar relatórios para '{company_name}' no ano {year+1}")
        
        # Extrair links de todos os relatórios disponíveis
        dfp_links = self.extract_links_from_select(url_report)
        if not dfp_links:
            raise ReportNotFoundException(f"Não foi possível extrair links dos relatórios para '{company_name}'")
        
        # Extrair links de ITRs
        itr_links = self.get_all_itr_links(data_json_current)
        
        # Processar dados trimestrais
        quarterly_data = {}
        
        # Processar ITRs em paralelo
        def process_itr(date_link):
            date, link = date_link
            try:
                links = self.extract_links_from_select(link)
                if links:
                    reports = self.fetch_all_reports(link, links, report_types)
                    return date, self.transform_report_dates(reports, year)
                return date, None
            except Exception as e:
                logger.error(f"Erro ao processar ITR de {date}: {e}")
                return date, None
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            itr_results = list(executor.map(process_itr, itr_links.items()))
            
        for date, reports in itr_results:
            if reports:
                quarterly_data[date] = reports
        
        # Adicionar DFP (relatório de fim de ano)
        try:
            end_year_date = f'{year}-12-31'
            end_year_data = self.fetch_all_reports(url_report, dfp_links, report_types)
            quarterly_data[end_year_date] = self.transform_report_dates(end_year_data, year)
        except Exception as e:
            logger.error(f"Erro ao processar DFP de {year}-12-31: {e}")
        
        return quarterly_data
    
    def to_json(self, data: Dict) -> str:
        """
        Converte os dados para formato JSON
        
        Args:
            data: Dicionário com os dados a serem convertidos
            
        Returns:
            String JSON com os dados
        """
        json_data = {}
        
        for date_key, reports in data.items():
            json_data[date_key] = {}
            
            for report_type, df in reports.items():
                if df is not None and not df.empty:
                    json_data[date_key][report_type] = df.to_dict()
        
        return json.dumps(json_data)
    
    def export_to_csv(self, data: Dict, output_dir: str) -> Dict[str, str]:
        """
        Exporta os dados para arquivos CSV
        
        Args:
            data: Dicionário com os dados a serem exportados
            output_dir: Diretório para salvar os arquivos
            
        Returns:
            Dicionário com caminhos dos arquivos salvos
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        file_paths = {}
        
        for date_key, reports in data.items():
            date_dir = os.path.join(output_dir, date_key)
            if not os.path.exists(date_dir):
                os.makedirs(date_dir)
                
            for report_type, df in reports.items():
                if df is not None and not df.empty:
                    safe_name = report_type.replace("/", "_").replace(" ", "_")
                    file_path = os.path.join(date_dir, f"{safe_name}.csv")
                    df.to_csv(file_path)
                    file_paths[f"{date_key}/{report_type}"] = file_path
        
        return file_paths

# Exemplo de uso
if __name__ == "__main__":
    # Caminho para o CSV de empresas
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    df_companies = B3DataFetcher().get_all_listed_companies()
    extractor = B3Extractor(df_companies=df_companies, max_workers=8)
    
    try:
        # Extrair dados da PETROBRAS para 2023
        data = extractor.fetch_quarterly_data("PETR", 2023)
        
        # Exportar para CSV
        file_paths = extractor.export_to_csv(data, os.path.join(BASE_DIR, 'output', 'PETR_2023'))
        print(f"Dados exportados para {len(file_paths)} arquivos")
        
        # Ou exportar para JSON
        json_data = extractor.to_json(data)
        with open(os.path.join(BASE_DIR, 'output', 'PETR_2023.json'), 'w', encoding='utf-8') as f:
            f.write(json_data)
        print("Dados exportados para JSON")
        
    except Exception as e:
        logger.error(f"Erro ao extrair dados: {e}")