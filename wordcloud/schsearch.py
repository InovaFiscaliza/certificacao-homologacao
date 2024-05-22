# Author    : Maxwel de Souza Freitas
import configparser
import json
import os
import pandas as pd
import re
import requests
import string
import uuid


from collections import Counter
from datetime import datetime, timedelta
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from pathlib import Path
from unicodedata import normalize
from urllib.parse import urlparse

# CONSTANTS

# time format for json result files
RESULT_TS_FORMAT = '%Y%m%d%H%M%S%f'
# time format for wordcloud files
ANNOTATITION_TS_FORMAT = '%d/%m/%Y %H:%M:%S'

#output folders
SEARCH_RESULTS_FOLDER = 'datasets/searchresults'
ANNOTATITIONS_FOLDER = 'datasets/annotations'

# sites to ignore results
BLACK_LISTED_SITES = ['gov.br', 'fccid.io', 'wer-hat-angerufen.info']


 # SCH DATABASE
 
def load_sch(file_sch,results_folder=SEARCH_RESULTS_FOLDER,grace_period=0):
    
    # load SCH database
    usecols = [0,1,11,12,13,14,15]
    dtype = {'Número de Homologação': 'str'}
    parse_dates = [0]
    date_format = '%d/%m/%Y'
    
    df_sch = pd.read_csv(file_sch,sep=';',usecols=usecols,dtype=dtype,parse_dates=parse_dates,date_format=date_format)
    df_sch = df_sch[df_sch['Categoria do Produto']==2]
    df_sch = df_sch.sort_values(by='Data da Homologação',ascending=False)
    df_sch = df_sch.drop_duplicates(subset='Número de Homologação')

    # load search history
    results_files = list(Path(results_folder).glob('*.json'))
    search_history = []
    for file in results_files:
        search_date, search_engine, search_term, _ = re.split('[_.]',file.name)
        search_date = datetime.strptime(search_date,RESULT_TS_FORMAT).date()
        search_history.append([search_term,search_date])
    columns = ['Número de Homologação', 'Última Pesquisa']
    df_search_history = pd.DataFrame(search_history,columns=columns)
    df_search_history = df_search_history.sort_values(by=columns,ascending=[True,False])
    df_search_history = df_search_history.drop_duplicates(subset='Número de Homologação')

    # merge both dataframes
    df_sch = df_sch.merge(df_search_history,how='left').fillna(-1)

    # filter products certifieds before grace period
    if grace_period > 0:
        certification_date_limit = datetime.today().date() - timedelta(days=grace_period)
        certification_date_limit = certification_date_limit.strftime('%Y-%m-%d')
        df_sch = df_sch[df_sch['Data da Homologação']<=certification_date_limit]

    return df_sch 

# CREDENTIALS

class SCHWebSearch(object):
    
    def __init__(self, search_results_folder=SEARCH_RESULTS_FOLDER, creds_file=None):
        
        if creds_file == None:
            creds_file = Path(os.environ['USERPROFILE'],'creds.ini')
            
        if not os.path.exists(creds_file):
            raise FileNotFoundError('Credentials file not found', creds_file)

        creds = configparser.ConfigParser()
        creds.read(creds_file)        
        self.bing_search_api_key = creds['BING_SEARCH']['bing_search_api_key']
        self.bing_search_endpoint = creds['BING_SEARCH']['bing_search_endpoint']
        self.google_search_api_key = creds['GOOGLE_SEARCH']['google_search_api_key']
        self.google_search_engine_id = creds['GOOGLE_SEARCH']['google_search_engine_id']
        self.google_search_endpoint = creds['GOOGLE_SEARCH']['google_search_endpoint']
        self.search_results_folder = search_results_folder
        
    def save_results_file(self, search_results):
        
        if 'kind' in search_results.keys(): # Google
            search_engine = 'GOOGLE'
            search_term = search_results['queries']['request'][0]['searchTerms']
        
        elif '_type' in search_results.keys(): # Bing
            search_engine = 'BING'
            search_term = search_results['queryContext']['originalQuery']
        
        else:
            return None
                
        result_ts = datetime.now().strftime(RESULT_TS_FORMAT)
        results_filename = f'{result_ts}_{search_engine}_{search_term}.json'
        file_to_save = Path(self.search_results_folder,results_filename)
           
        with open(file_to_save, 'w') as f:
            json.dump(search_results,f, indent=2)    

        return file_to_save
        
    def google_search(self, search_term):

        # search params
        params = {
            'q': search_term,
            'key': self.google_search_api_key, 
            'cx': self.google_search_engine_id, 
            'count': 50,
            'cr': 'countryBR', 
            'lr': 'lang_pt'
            }
        # execute query
        try:
            response = requests.get(self.google_search_endpoint, params=params)
            response.raise_for_status()
            query_raw_results = response.json()
            result_file = self.save_results_file(query_raw_results)
        except Exception as ex:
            raise ex
                
        return response.status_code, result_file
    
    def bing_search(self, search_term):
        # search params
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
        # execute query
        try:
            response = requests.get(self.bing_search_endpoint, headers=headers, params=params)
            response.raise_for_status()
            query_raw_results = response.json()
            result_file = self.save_results_file(query_raw_results)
        except Exception as ex:
            raise ex
        
        return response.status_code, result_file



