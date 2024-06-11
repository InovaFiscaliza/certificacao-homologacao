#%% Author    : Maxwel de Souza Freitas
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
from pathlib import Path
from unicodedata import normalize
from urllib.parse import urlparse

#%% CONSTANTS

# time format for json result files
RESULT_TS_FORMAT = '%Y%m%d%H%M%S%f'
# time format for wordcloud files
ANNOTATION_TS_FORMAT = '%d/%m/%Y %H:%M:%S'
ANOTATION_FILE_TS_FORMAT = '%Y.%m.%d_T%H.%M.%S'

 #%% SCH DATABASE
 
def load_sch(sch_database_file,parsed_results_folder=None,grace_period=0):
    
    # load SCH database
    usecols = [0,1,11,12,13,14,15]
    dtype = {'Número de Homologação': 'str'}
    parse_dates = [0]
    date_format = '%d/%m/%Y'
    
    df_sch = pd.read_csv(
        sch_database_file,
        sep=';',
        usecols=usecols,
        dtype=dtype,
        parse_dates=parse_dates,
        date_format=date_format
        )
    
    df_sch = df_sch[df_sch['Categoria do Produto']==2]
    df_sch = df_sch.sort_values(by='Data da Homologação',ascending=False)
    df_sch = df_sch.drop_duplicates(subset='Número de Homologação')
    
    # filter products certifieds before grace period
    if grace_period > 0:
        certification_date_limit = datetime.today().date() - timedelta(days=grace_period)
        certification_date_limit = certification_date_limit.strftime('%Y-%m-%d')
        df_sch = df_sch[df_sch['Data da Homologação']<=certification_date_limit]

    # load search history
    if parsed_results_folder is not None:
        results_files = list(Path(parsed_results_folder).glob('*.json'))
        search_history = []
        for file in results_files:
            search_date, _ , search_term, _, _ = re.split('[_.]',file.name)
            search_date = datetime.strptime(search_date,RESULT_TS_FORMAT).date()
            search_history.append([search_term,search_date])
            
        columns = ['Número de Homologação', 'Última Pesquisa']
        df_search_history = pd.DataFrame(search_history,columns=columns)
        df_search_history = df_search_history.sort_values(by=columns,ascending=[True,False])
        df_search_history = df_search_history.drop_duplicates(subset='Número de Homologação')

        # merge both dataframes
        df_sch = df_sch.merge(df_search_history,how='left').fillna(-1)
    else:
        df_sch['Última Pesquisa'] = -1
          
    df_sch = df_sch.reset_index(drop=True)
    
    return df_sch 

#%% SEARCH CLASS

class SCHWebSearch(object):
    
    def __init__(self, search_results_folder, creds_file=None):
        
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
            return response.status_code, None
                
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
            return response.status_code, None
        
        return response.status_code, result_file

#%% ANNOTATION

def tokenizer(doc,normalize_words=False):

    stop_words = stopwords.words('portuguese')
    stop_words.extend(stopwords.words('english'))
    stop_words.extend(list(string.punctuation))

    # stopwords específicas do domínio
    stop_words.extend(['cm', 'feature', 'features', 'informações', 'itens', 'leve', 'list', 'nulo', 'package', 
                       'pacote', 'pacotes', 'recurso', 'tamanho', 'ver', 'anatel', 'laranja', '.', '...',
                       'complementares', 'peça'])

    doc = doc.lower()
    if normalize_words:
        doc = normalize('NFKD', doc).encode('ASCII', 'ignore').decode('ASCII')

    # CountVectorizer token pattern
    pattern = r'\b\w\w+\b'
    tokens = [token for token in re.findall(pattern,doc) if token.lower() not in stop_words]
    # remove number-only tokens
    tokens = [token for token in tokens if not re.match('\d+',token)]

    return tokens


def is_black_listed_site(url,blacklisted_sites=None):
        if blacklisted_sites is not None:
            for site in blacklisted_sites:
                if site in url:
                    return True
        return False
    
    
def parse_result_file(file, parsed_results_folder=None, error_results_folder=None, max_words=25):

    parse_url = lambda url: '.'.join(urlparse(url).netloc.split('.')[-3:])
    search_result_id = str(uuid.uuid4())

    EMPTY_RESULT = {'ID': search_result_id, 
                    'DataHora': None,
                    'Computador': None,
                    'Usuário': None, 
                    'Homologação': None, 
                    'Atributo': None,
                    'Valor': None,  
                    'Situação': -1}

    if isinstance(file, str):
        file = Path(file)
        
    # check parsed results folder and file
    # if parse results folder wasn't declared set default
    if parsed_results_folder is None:
        parsed_results_folder = file.parents[0] / 'parsed_results'
    # create parsed results folder, if it doesn't exists    
    if not parsed_results_folder.exists():
        parsed_results_folder.mkdir(parents=True, exist_ok=True)
    # set parsed results file
    parsed_file = parsed_results_folder / f'{file.stem}_{search_result_id}{file.suffix}'
    
    # check error results folder and file
    # if parse error folder wasn't declared set default
    if error_results_folder is None:
        error_results_folder = file.parents[0] / 'error_results'
    # create parsed results folder, if it doesn't exists    
    if not error_results_folder.exists():
        error_results_folder.mkdir(parents=True, exist_ok=True)
    # set parsed results file
    error_file = error_results_folder / f'{file.stem}_{search_result_id}{file.suffix}'
    
    search_date, search_engine, search_term, _ = re.split('[_.]',file.name)
    search_site = None
    
    try:
        with open(file) as f:
            results = json.load(f)
        # move parsed result file
        file.rename(parsed_file)
    except:
        file.rename(error_file)
        return EMPTY_RESULT

    try:
        with open('blacklisted_sites.txt') as f:
            blacklisted_sites = f.readlines()
            blacklisted_sites = [site.strip() for site in blacklisted_sites]
    except:
        blacklisted_sites = None
        
    lines = []
    if search_engine == 'GOOGLE':
        # results without items in keys are empty
        if 'items' in results.keys():
            for item in results['items']:
                search_site = item['displayLink']
                if is_black_listed_site(search_site):
                    continue
                else:
                    search_site = parse_url(search_site)
                    lines.append(item['title'])
                    if 'snippet' in item.keys():
                        lines.append(item['snippet'])
        else:
            return EMPTY_RESULT
                        
    elif search_engine == 'BING':
        # results without webPages in keys are empty
        if 'webPages' in results.keys():
            for item in results['webPages']['value'][:10]:
                search_site = item['url']
                if is_black_listed_site(search_site):
                    continue
                else:
                    search_site = parse_url(search_site)
                    lines.append(item['name'])
                    if 'snippet' in item.keys():
                        lines.append(item['snippet'])
        else:
            return EMPTY_RESULT
                 
    if len(lines) >= 1:
        words = tokenizer(' '.join(lines))
        words_conter = Counter(words)
        wordCloud_dict = {key:value for key,value in words_conter.most_common(max_words)}
        wordCloud_json = json.dumps(wordCloud_dict, ensure_ascii=False)
        situacao = 1
    else:
        wordCloud_json = ''
        situacao = -1
        
    wourdCloudInfo_dict = {
        'metaData': {
            'Version': 1,
            'Source': search_engine,
            'Mode': 'API',
            'Fields': ['Name', 'Snippet'],
            'nWords': max_words
        },
        'searchedWord': search_term,
        'cloudOfWords': wordCloud_json
        }
    
    wourdCloudInfo_json = json.dumps(wourdCloudInfo_dict, ensure_ascii=False)
    
    return {'ID': search_result_id,
            'DataHora': datetime.strptime(search_date,RESULT_TS_FORMAT).strftime(ANNOTATION_TS_FORMAT),
            'Computador': os.environ['COMPUTERNAME'],
            'Usuário': os.environ['USERNAME'], 
            'Homologação': f'{search_term[:5]}-{search_term[5:7]}-{search_term[-5:]}', 
            'Atributo': 'WordCloud',
            'Valor': wourdCloudInfo_json,  
            'Situação': situacao}

    
def save_annotation_file(df, annotation_folder):

    if isinstance(annotation_folder,str):
        annotation_folder = Path(annotation_folder)
        
    if df.empty:
        return -1

    annotation_ts = datetime.now().strftime(ANOTATION_FILE_TS_FORMAT)
    annotation_file = f'Annotation_{annotation_ts}.xlsx'
    annotation_file = Path(annotation_folder,annotation_file)

    try:
        df.to_excel(annotation_file,index=False)
        print(f'Annotation file saved: {annotation_file}')
    except Exception as ex:
        print(f'Annotation file not saved: {annotation_file}')
        raise ex

    return 0 

