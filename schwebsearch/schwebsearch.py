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
# UUID4 regex validation pattern
UUID4RE = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}',re.I)

 #%% SCH DATABASE
 
def load_sch(sch_database_file,search_history_folder=None):
    
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

    df_modelo = df_sch[['Número de Homologação','Modelo']].dropna()
    df_modelo = df_modelo.groupby('Número de Homologação',as_index=False)['Modelo'].apply(lambda x: ' | '.join(x))

    df_nome_comercial = df_sch[['Número de Homologação','Nome Comercial']].dropna()
    df_nome_comercial = df_nome_comercial.groupby('Número de Homologação',as_index=False)['Nome Comercial'].apply(lambda x: ' | '.join(x))

    columns_to_keep = ['Data da Homologação', 'Número de Homologação', 'Nome do Fabricante', 'Categoria do Produto', 'Tipo do Produto']
    df_sch = df_sch[columns_to_keep]
    df_sch = df_sch.drop_duplicates(subset='Número de Homologação')

    df_sch = df_sch.merge(df_modelo,how='left')
    df_sch = df_sch.merge(df_nome_comercial,how='left')
    df_sch = df_sch.fillna('')
    
    # load search history
    if search_history_folder is not None:
        if isinstance(search_history_folder, str):
            search_history_folder = Path(search_history_folder)

        search_history = []
        search_history_files = [file for file in search_history_folder.glob('*.json') if re.search(UUID4RE,file.name)]           
        if len(search_history_files) > 0:
            for file in search_history_files:
                search_date, search_engine, search_term, search_id, _ = re.split('[_.]',file.name)
                search_date = datetime.strptime(search_date,RESULT_TS_FORMAT).date()
                search_metadata = {'Última Pesquisa': search_date, 
                                   'Mecanismo de Busca': search_engine,
                                   'Id de Busca': search_id, 
                                   'Número de Homologação': search_term}
                search_history.append(search_metadata)
            df_search_history = pd.DataFrame(search_history)
            
            # merge sch and search history dataframes
            df_sch = df_sch.merge(df_search_history, how='left')
            columns_to_fill = ['Última Pesquisa', 'Mecanismo de Busca', 'Id de Busca']
            df_sch[columns_to_fill] = df_sch[columns_to_fill].fillna(-1)
            
        # no files in search history folder    
        else:
            df_sch['Última Pesquisa'] = -1
            df_sch['Mecanismo de Busca'] = -1
            df_sch['Id de Busca'] = -1
            
    # no search history folder
    else:
        df_sch['Última Pesquisa'] = -1
        df_sch['Mecanismo de Busca'] = -1
        df_sch['Id de Busca'] = -1
          
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
    
    
def parse_result_file(file, search_history_folder=None, parse_errors_folder=None, max_words=25):

    parse_url = lambda url: '.'.join(urlparse(url).netloc.split('.')[-3:])
    
    if isinstance(file, str):
        file = Path(file)
    
    if re.search(UUID4RE,file.name):
        search_date, search_engine, search_term, search_result_id, _ = re.split('[_.]',file.name)
        move_file = False
    else:
        search_date, search_engine, search_term, _ = re.split('[_.]',file.name)
        search_result_id = str(uuid.uuid4())
        move_file = True
    search_site = None    

    EMPTY_RESULT = {'ID': search_result_id, 
                    'DataHora': None,
                    'Computador': None,
                    'Usuário': None, 
                    'Homologação': None, 
                    'Atributo': None,
                    'Valor': None,  
                    'Situação': -1}  
        
    # check parsed results folder and file
    # if parse results folder wasn't declared set default
    if search_history_folder is None:
        search_history_folder = file.parents[0] / 'search_history'
    # create parsed results folder, if it doesn't exists    
    if not search_history_folder.exists():
        search_history_folder.mkdir(parents=True, exist_ok=True)
    # set parsed results file
    parsed_file = search_history_folder / f'{file.stem}_{search_result_id}{file.suffix}'
    
    # check error results folder and file
    # if parse error folder wasn't declared set default
    if parse_errors_folder is None:
        parse_errors_folder = file.parents[0] / 'parse_errors'
    # create parsed results folder, if it doesn't exists    
    if not parse_errors_folder.exists():
        parse_errors_folder.mkdir(parents=True, exist_ok=True)
    # set parsed results file
    error_file = parse_errors_folder / f'{file.stem}_error{file.suffix}'

    try:
        with open(file) as f:
            results = json.load(f)
        # move parsed result file
        if move_file:
            file.rename(parsed_file)
    except:
        # move parsed error file
        if move_file:
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
            for item in results['webPages']['value']:
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

    
def save_annotation_file(df, annotation_folder, actual_annotation_file=None):

    if isinstance(annotation_folder,str):
        annotation_folder = Path(annotation_folder)
        
    if actual_annotation_file is not None:
        if isinstance(actual_annotation_file,str):
            actual_annotation_file = Path(actual_annotation_file)
        
        if actual_annotation_file.exists():
            df_actual_annotation = pd.read_excel(actual_annotation_file)
            columns_to_keep = ['ID']
            df_actual_annotation = df_actual_annotation[columns_to_keep]
            df_actual_annotation.columns = ['actual_ID']
            df = df.merge(df_actual_annotation,left_on='ID',right_on='actual_ID',how='left')
            df = df[df['actual_ID'].isna()].iloc[:,:-1]
            
    if not df.empty:    
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
    
    else:
        print('No new annotation to save')
        return -1

