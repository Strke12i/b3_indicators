import base64
import pandas as pd
import cloudscraper


class B3DataFetcher:
    """
    Classe para buscar dados da B3, incluindo informações sobre empresas listadas.
    """
    BASE_URL = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'

    def __init__(self):
        """
        Inicializa o scraper.
        """
        self.scraper = cloudscraper.create_scraper()

    def _encode_payload(self, page: int, page_size: int = 100, language: str = "pt-br") -> str:
        """
        Codifica o payload para a requisição em Base64.
        """
        payload = f'{{"language":"{language}","pageNumber":{page},"pageSize":{page_size}}}'
        base64_bytes = base64.b64encode(payload.encode('ascii'))
        return str(base64_bytes, 'ascii')

    def _fetch_page(self, page: int) -> dict:
        """
        Busca os dados de uma única página.
        """
        encoded_payload = self._encode_payload(page)
        url = self.BASE_URL + encoded_payload
        response = self.scraper.get(url)

        if response.status_code != 200:
            raise Exception(f"Erro na requisição. Status code: {response.status_code}")
        
        return response.json()

    def get_all_listed_companies(self) -> pd.DataFrame:
        """
        Busca todas as empresas listadas na B3 e retorna um DataFrame com os resultados.
        """
        # Busca a primeira página para determinar o número total de páginas
        first_page_data = self._fetch_page(1)
        total_pages = first_page_data['page']['totalPages']
        all_results = first_page_data['results']

        # Itera pelas páginas restantes
        for page in range(2, total_pages + 1):
            page_data = self._fetch_page(page)
            all_results.extend(page_data['results'])

        # Converte para DataFrame
        return pd.DataFrame(all_results)


if __name__ == "__main__":
    fetcher = B3DataFetcher()
    companies_df = fetcher.get_all_listed_companies()

    # Exibir as primeiras linhas do DataFrame
    print(companies_df.head())

    # Exemplo de busca de uma empresa específica
    empresa = "WEGE"
    print(companies_df[companies_df['issuingCompany'] == empresa])
