#%%
import configparser
import json
import os
import pandas as pd
import requests

from datetime import datetime

#%%
class WebSearchEngine():
    def __init__(self, creds_file='creds.ini'):
        try:
            creds = configparser.ConfigParser()
            creds.read(creds_file)
            self.bing_search_api_key = creds['BING_SEARCH']['bing_search_api_key']
            self.bing_search_endpoint = creds['BING_SEARCH']['bing_search_endpoint']
            self.google_search_api_key = creds['GOOGLE_SEARCH']['google_search_api_key']
            self.google_search_engine_id = creds['GOOGLE_SEARCH']['google_search_engine_id']
            self.google_search_endpoint = creds['GOOGLE_SEARCH']['google_search_endpoint']
        except FileNotFoundError as e:
            e.strerror = 'The credentials file does not exist or is corrupted.'
            raise e
                  
    def bing_search(self,search_term):
        headers = {'Ocp-Apim-Subscription-Key': self.bing_search_api_key}
        params = {'q': search_term,
                # A 2-character country code of the country where the results come from.
                'cc': 'BR',
                # The number of search results to return in the response. 
                # The default is 10 and the maximum value is 50. 
                # he actual number delivered may be less than requested.
                'count': 50,
                # The market where the results come from.
                'mkt': 'pt-BR',
                # A comma-delimited list of answers to include in the response.
                'responseFilter': 'Webpages',
                }
        
        try:
            response = requests.get(self.bing_search_endpoint, headers=headers, params=params)
            response.raise_for_status()
            query_raw_results = response.json()
            original_query = query_raw_results['queryContext']['originalQuery']

            query_clean_results = []
            if query_raw_results['rankingResponse'] and 'webPages' in query_raw_results.keys():
                for item in query_raw_results['webPages']['value']:
                    name = item['name']
                    url = item['url']
                    if 'snippet' in item.keys():
                        snippet = item['snippet']
                    else:
                        snippet = None
                    query_clean_results.append({'search_provider': 'Bing',
                                                'original_query': original_query,
                                                'name': name,
                                                'url': url,
                                                'snippet':snippet})
            else:
                 query_clean_results = [{'search_provider': 'Bing',
                                         'original_query': original_query,
                                         'name': None,
                                         'url': None,
                                         'snippet': None}]
        
            return query_clean_results,query_raw_results
        except Exception as ex:
            raise ex
        
    def google_search(self,search_term):
       
        params = {'q': search_term,
                  'key': self.google_search_api_key, 
                  'cx': self.google_search_engine_id, 
                  'count': 50,
                  'cr': 'countryBR', 
                  'lr': 'lang_pt'}
    
        try:
            response = requests.get(self.google_search_endpoint, params=params)
            response.raise_for_status()
            query_raw_results = response.json()
            original_query = query_raw_results['queries']['request'][0]['searchTerms']
            totalResults = int(query_raw_results['searchInformation']['totalResults'])

            query_clean_results = []
            if totalResults > 0:
                for item in query_raw_results['items']:
                    name = item['title']
                    url = item['link']
                    if 'snippet' in item.keys():
                        snippet = item['snippet']
                    else:
                        snippet = None
                    
                    query_clean_results.append({'search_provider': 'Google',
                                                'original_query': original_query,
                                                'name': name,
                                                'url': url,
                                                'snippet': snippet})
            else:
                query_clean_results.append({'search_provider': 'Google',
                                            'original_query': original_query,                     
                                            'name': None,
                                            'url': None,
                                            'snippet': None})
                
            return query_clean_results,query_raw_results
        except Exception as ex:
            raise ex

#%%
class WebSearchDataManager():
    
    def __init__(self, datasets_dir='datasets/search_results', raw_results_dir='datasets/search_results/raw'):
        
        if not os.path.exists(datasets_dir):
            os.makedirs(datasets_dir)
        if not os.path.exists(raw_results_dir):
            os.makedirs(raw_results_dir)
            
        self.datasets_dir = datasets_dir
        self.raw_search_results_dir = raw_results_dir
        
        self.clean_search_results = []
        self.raw_search_results = []
    
    def update_search_results(self, search_results):        
        self.clean_search_results.extend(search_results)
        
    def load_search_results(self,dataset_file_name='products_search_results.parquet'):
        # check if a complete file path was provided
        # otherwise, search for the file in datasets_dir
        if not os.path.exists(dataset_file_name):
            clean_search_results_file = os.path.join(self.datasets_dir,dataset_file_name)
            
        if os.path.exists(clean_search_results_file):
            df = pd.read_parquet(clean_search_results_file)
            self.clean_search_results = df.to_dict('records')
    
    def save_clean_search_results(self,dataset_file_name='products_search_results.parquet'):
        file_to_save = os.path.join(self.datasets_dir,dataset_file_name)
        df = pd.DataFrame(self.clean_search_results)
        subset=['search_provider','original_query','url']
        df = df.drop_duplicates(subset=subset)
        df.to_parquet(file_to_save)
        
    def save_raw_search_results(self,json_search_results):
        file_exists = True
        while file_exists:
            filename = '{}.json'.format(datetime.now().strftime('%Y%m%d%H%M%S%f'))
            file_to_save = os.path.join(self.raw_search_results_dir,filename)
            file_exists = os.path.exists(file_to_save)
        with open(file_to_save, 'w') as f:
            json.dump(json_search_results,f,indent=2)
        