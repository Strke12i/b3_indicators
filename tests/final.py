import pandas as pd
import zipfile
import requests
from io import BytesIO, StringIO
import numpy as np
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import time
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass

from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.pool import NullPool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('cvm_pipeline')

# Data class to represent a financial report
@dataclass
class FinancialReport:
    report_type: str
    dataframe: pd.DataFrame


class CVMDataPipeline:
    """Pipeline for downloading and processing CVM financial data"""
    
    def __init__(self, db_connection_string: str, max_workers: int = 4):
        """
        Initialize the pipeline
        
        Args:
            db_connection_string: SQLAlchemy connection string
            max_workers: Maximum number of threads for parallel processing
        """
        self.db_connection_string = db_connection_string
        self.max_workers = max_workers
        self.report_types = {
            'BPA': 'Balanço Patrimonial Ativo',
            'BPP': 'Balanço Patrimonial Passivo',
            'DRE': 'Demonstração do Resultado',
            'DFC_MI': 'Demonstração do Fluxo de Caixa'
        }
        
        # Create tables metadata once at initialization
        self.engine = create_engine(db_connection_string, poolclass=NullPool)
        self.metadata = MetaData(bind=self.engine)
        self.tables = {
            'empresa': Table('empresa', self.metadata, autoload_with=self.engine),
            'relatorio': Table('relatorio', self.metadata, autoload_with=self.engine),
            'dados_relatorio': Table('dados_relatorio', self.metadata, autoload_with=self.engine)
        }
    
    def download_data(self, url: str) -> bytes:
        """
        Download data from a URL
        
        Args:
            url: URL to download from
            
        Returns:
            Response content as bytes
        """
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"Error downloading data from {url}: {e}")
            raise
    
    def process_zip_file(self, content: bytes, file_pattern: str, report_type: str, year: int) -> pd.DataFrame:
        """
        Process a ZIP file containing CVM data
        
        Args:
            content: ZIP file content as bytes
            file_pattern: Pattern to match filename in ZIP
            report_type: Type of financial report
            year: Year of the data
            
        Returns:
            DataFrame with the processed data
        """
        try:
            with zipfile.ZipFile(BytesIO(content)) as zip_file:
                file_name = next((name for name in zip_file.namelist() if file_pattern in name), None)
                
                if not file_name:
                    raise ValueError(f"File pattern {file_pattern} not found in ZIP")
                
                df = pd.read_csv(
                    StringIO(zip_file.read(file_name).decode("latin1")),
                    sep=";",
                    encoding="latin1",
                    dtype={
                        'CD_CVM': str,
                        'CD_CONTA': str,
                        'VL_CONTA': str
                    },
                    low_memory=False
                )
                
                # Add report group information for DFC_MI
                if report_type == 'DFC_MI':
                    df["GRUPO_DFP"] = f"DF Consolidado - {self.report_types[report_type]}"
                
                return df
                
        except Exception as e:
            logger.error(f"Error processing ZIP file for {report_type} {year}: {e}")
            raise
    
    def get_financial_data(self, year: int, data_type: str = 'dfp') -> Dict[str, pd.DataFrame]:
        """
        Download and process financial data for a specific year
        
        Args:
            year: Year to fetch data for
            data_type: Type of data to fetch ('dfp' or 'itr')
            
        Returns:
            Dictionary of DataFrames with the processed data
        """
        url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/{data_type.upper()}/DADOS/{data_type}_cia_aberta_{year}.zip"
        
        try:
            logger.info(f"Downloading {data_type.upper()} data for {year}")
            content = self.download_data(url)
            
            results = {}
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                
                for report_type in self.report_types.keys():
                    file_pattern = f"{data_type}_cia_aberta_{report_type}_con_{year}.csv"
                    futures[executor.submit(self.process_zip_file, content, file_pattern, report_type, year)] = report_type
                
                for future in as_completed(futures):
                    report_type = futures[future]
                    try:
                        results[report_type] = future.result()
                    except Exception as e:
                        logger.error(f"Error processing {report_type} for {year}: {e}")
            
            return results
        
        except Exception as e:
            logger.error(f"Error fetching {data_type} data for {year}: {e}")
            return {}
    
    def preprocess_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Preprocess a DataFrame to standardize data types and clean values
        
        Args:
            df: DataFrame to preprocess
            
        Returns:
            Preprocessed DataFrame
        """
        # Make a copy to avoid modifying the original
        df = df.copy()
        
        # Convert CD_CVM to integer
        df['CD_CVM'] = df['CD_CVM'].str.replace(',', '').astype(float).astype(int)
        
        # Handle dates
        if 'DT_INI_EXERC' in df.columns:
            df['DT_INI_EXERC'] = pd.to_datetime(df['DT_INI_EXERC'], errors='coerce')
        else:
            df['DT_INI_EXERC'] = None
            
        df['DT_FIM_EXERC'] = pd.to_datetime(df['DT_FIM_EXERC'], errors='coerce')
        
        # Convert CD_CONTA to integer
        df['CD_CONTA'] = df['CD_CONTA'].str.replace('.', '').str.replace(',', '').astype(int)
        
        # Convert VL_CONTA to float
        df['VL_CONTA'] = df['VL_CONTA'].str.replace(',', '.').astype(float)
        
        return df
    
    def _create_batch_inserts(self, df: pd.DataFrame, batch_size: int = 1000):
        """Generator to yield batches of a dataframe for processing"""
        for start_idx in range(0, len(df), batch_size):
            yield df.iloc[start_idx:start_idx + batch_size]
    
    def insert_empresas(self, session, df: pd.DataFrame) -> set:
        """
        Insert unique companies into the database
        
        Args:
            session: SQLAlchemy session
            df: DataFrame with company data
            
        Returns:
            Set of existing company IDs
        """
        # Get unique companies from the dataframe
        unique_companies = df[['CD_CVM', 'DENOM_CIA']].drop_duplicates()
        
        # Get existing companies from the database
        existing_empresas = {int(e.id_empresa) for e in 
                            session.query(self.tables['empresa'].c.id_empresa).filter(
                                self.tables['empresa'].c.id_empresa.in_(unique_companies['CD_CVM'].tolist())
                            ).all()}
        
        # Prepare companies to insert
        empresas_to_insert = [
            {"id_empresa": int(row['CD_CVM']), "nome_empresa": str(row['DENOM_CIA'])}
            for _, row in unique_companies.iterrows()
            if int(row['CD_CVM']) not in existing_empresas
        ]
        
        # Insert companies in batches
        if empresas_to_insert:
            for batch in self._create_batch_inserts(pd.DataFrame(empresas_to_insert)):
                try:
                    stmt = insert(self.tables['empresa']).values(batch.to_dict('records'))
                    stmt = stmt.on_conflict_do_nothing(index_elements=['id_empresa'])
                    session.execute(stmt)
                    session.commit()
                except Exception as e:
                    logger.error(f"Error inserting companies: {e}")
                    session.rollback()
        
        return existing_empresas
    
    def insert_relatorios(self, session, df: pd.DataFrame) -> dict:
        """
        Insert reports into the database
        
        Args:
            session: SQLAlchemy session
            df: DataFrame with report data
            
        Returns:
            Dictionary mapping report keys to report IDs
        """
        # Extract report type from GRUPO_DFP
        nome_relatorio = df.iloc[0]["GRUPO_DFP"].split(" - ")[1]
        
        # Group by company and dates
        group_columns = ['CD_CVM']
        if 'DT_INI_EXERC' in df.columns and not df['DT_INI_EXERC'].isna().all():
            group_columns.append('DT_INI_EXERC')
        group_columns.append('DT_FIM_EXERC')
        
        # Get unique report combinations
        unique_combinations = df[group_columns].drop_duplicates()
        
        # Prepare reports to insert
        relatorios_to_insert = []
        for _, row in unique_combinations.iterrows():
            relatorios_to_insert.append({
                "tipo_relatorio": nome_relatorio,
                "id_empresa": int(row['CD_CVM']),
                "data_inicio": row.get('DT_INI_EXERC'),
                "data_fim": row['DT_FIM_EXERC']
            })
        
        # Insert reports in batches
        if relatorios_to_insert:
            for batch in self._create_batch_inserts(pd.DataFrame(relatorios_to_insert)):
                try:
                    stmt = insert(self.tables['relatorio']).values(batch.to_dict('records'))
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['tipo_relatorio', 'id_empresa', 'data_inicio', 'data_fim']
                    )
                    session.execute(stmt)
                    session.commit()
                except Exception as e:
                    logger.error(f"Error inserting reports: {e}")
                    session.rollback()
        
        # Get inserted report IDs
        inserted_relatorios = {}
        report_keys = []
        
        for _, row in unique_combinations.iterrows():
            data_inicio = row.get('DT_INI_EXERC')
            data_fim = row['DT_FIM_EXERC']
            
            # Convert to strings for querying
            data_inicio_str = data_inicio.strftime('%Y-%m-%d') if pd.notna(data_inicio) else None
            data_fim_str = data_fim.strftime('%Y-%m-%d') if pd.notna(data_fim) else None
            
            report_keys.append((nome_relatorio, int(row['CD_CVM']), data_inicio_str, data_fim_str))
        
        # Query for inserted report IDs
        for r in session.query(self.tables['relatorio']).filter(
            self.tables['relatorio'].c.tipo_relatorio == nome_relatorio,
            self.tables['relatorio'].c.id_empresa.in_([cd for _, cd, _, _ in report_keys])
        ).all():
            data_inicio = r.data_inicio.strftime('%Y-%m-%d') if r.data_inicio else None
            data_fim = r.data_fim.strftime('%Y-%m-%d') if r.data_fim else None
            key = (r.tipo_relatorio, int(r.id_empresa), data_inicio, data_fim)
            inserted_relatorios[key] = int(r.id_relatorio)
        
        return inserted_relatorios
    
    def insert_dados_relatorio(self, session, df: pd.DataFrame, inserted_relatorios: dict):
        """
        Insert report data into the database
        
        Args:
            session: SQLAlchemy session
            df: DataFrame with report data
            inserted_relatorios: Dictionary mapping report keys to report IDs
        """
        nome_relatorio = df.iloc[0]["GRUPO_DFP"].split(" - ")[1]
        
        # Group by company and dates
        group_columns = ['CD_CVM']
        if 'DT_INI_EXERC' in df.columns and not df['DT_INI_EXERC'].isna().all():
            group_columns.append('DT_INI_EXERC')
        group_columns.append('DT_FIM_EXERC')
        
        # Process in chunks to avoid memory issues
        for _, group_df in df.groupby(group_columns):
            cd_cvm = group_df['CD_CVM'].iloc[0]
            data_inicio = group_df['DT_INI_EXERC'].iloc[0] if 'DT_INI_EXERC' in group_df.columns else None
            data_fim = group_df['DT_FIM_EXERC'].iloc[0]
            
            # Convert to strings for dictionary lookup
            data_inicio_str = data_inicio.strftime('%Y-%m-%d') if pd.notna(data_inicio) else None
            data_fim_str = data_fim.strftime('%Y-%m-%d') if pd.notna(data_fim) else None
            
            relatorio_id = inserted_relatorios.get((nome_relatorio, int(cd_cvm), data_inicio_str, data_fim_str))
            
            if not relatorio_id:
                logger.warning(f"Report ID not found for {nome_relatorio}, {cd_cvm}, {data_inicio_str}, {data_fim_str}")
                continue
            
            # Prepare data to insert
            dados_to_insert = []
            for _, row in group_df.iterrows():
                dados_to_insert.append({
                    "id_relatorio": relatorio_id,
                    "codigo_conta": int(row["CD_CONTA"]),
                    "descricao": str(row["DS_CONTA"]),
                    "valor": float(row["VL_CONTA"])
                })
            
            # Insert data in batches
            for batch in self._create_batch_inserts(pd.DataFrame(dados_to_insert), batch_size=5000):
                try:
                    stmt = insert(self.tables['dados_relatorio']).values(batch.to_dict('records'))
                    stmt = stmt.on_conflict_do_update(
                        index_elements=['id_relatorio', 'codigo_conta'],
                        set_={'descricao': stmt.excluded.descricao, 'valor': stmt.excluded.valor}
                    )
                    session.execute(stmt)
                    session.commit()
                except Exception as e:
                    logger.error(f"Error inserting report data: {e}")
                    session.rollback()
    
    def process_dataframe(self, df: pd.DataFrame):
        """
        Process a DataFrame and insert data into the database
        
        Args:
            df: DataFrame to process
        """
        if df.empty:
            logger.warning("Empty dataframe, skipping processing")
            return
        
        # Preprocess data
        try:
            df = self.preprocess_data(df)
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            return
        
        # Create a new session for this dataframe
        Session = sessionmaker(bind=self.engine)
        session = Session()
        
        try:
            # Insert companies
            self.insert_empresas(session, df)
            
            # Insert reports and get IDs
            inserted_relatorios = self.insert_relatorios(session, df)
            
            # Insert report data
            self.insert_dados_relatorio(session, df, inserted_relatorios)
            
            logger.info(f"Successfully processed {df.iloc[0]['GRUPO_DFP']}")
        except Exception as e:
            logger.error(f"Error processing dataframe: {e}")
        finally:
            session.close()
    
    def run_for_year_range(self, start_year: int, end_year: int, data_types: List[str] = ['dfp', 'itr']):
        """
        Run the pipeline for a range of years
        
        Args:
            start_year: First year to process
            end_year: Last year to process
            data_types: List of data types to process ('dfp' and/or 'itr')
        """
        years = list(range(start_year, end_year + 1))
        
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(years))) as executor:
            futures = []
            
            for year in years:
                for data_type in data_types:
                    futures.append(executor.submit(self.process_year, year, data_type))
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in future: {e}")
    
    def process_year(self, year: int, data_type: str):
        """
        Process a single year of data
        
        Args:
            year: Year to process
            data_type: Type of data to process ('dfp' or 'itr')
        """
        logger.info(f"Processing {data_type.upper()} data for {year}")
        
        try:
            # Get data
            data_dict = self.get_financial_data(year, data_type)
            
            if not data_dict:
                logger.warning(f"No data found for {year} {data_type}")
                return
            
            # Process data in parallel
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                for report_type, df in data_dict.items():
                    futures.append(executor.submit(self.process_dataframe, df))
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Error processing dataframe for {year} {data_type}: {e}")
            
            logger.info(f"Finished processing {data_type.upper()} data for {year}")
        
        except Exception as e:
            logger.error(f"Error processing {year} {data_type}: {e}")


if __name__ == "__main__":
    # Example usage
    pipeline = CVMDataPipeline(
        db_connection_string='postgresql+psycopg2://admin:admin_password@localhost:5432/meu_banco',
        max_workers=8
    )
    
    # Process a range of years
    pipeline.run_for_year_range(2011, 2024)
    
    # Or process a specific year and data type
    # pipeline.process_year(2024, 'dfp')
    # pipeline.process_year(2024, 'itr')