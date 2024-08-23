import argparse
import configparser
import logging
import pandas as pd
import os.path as osp

from datetime import datetime, timedelta
from pathlib import Path
from tqdm.auto import tqdm

from schwebsearch import load_sch, SCHWebSearch, parse_result_file, save_annotation_file

logger = logging.getLogger(__name__)

def read_config_file(config_file='websearch_config.ini'):
    
    if osp.exists(config_file):
        logger.info('Reading config file ... ')
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
            sch_database_file = Path(config['SCHWEBSEARCH']['sch_database_file'])
            search_results_folder = Path(config['SCHWEBSEARCH']['search_results_folder'])
            search_history_folder = Path(config['SCHWEBSEARCH']['search_history_folder'])
            error_results_folder = Path(config['SCHWEBSEARCH']['error_results_folder'])
            annotation_folder = Path(config['SCHWEBSEARCH']['annotation_folder'])
            actual_annotation_file = Path(config['SCHWEBSEARCH']['actual_annotation_file'])
            
            logger.info(f'  sch_database_file: {sch_database_file}')
            logger.info(f'  search_results_folder: {search_results_folder}')
            logger.info(f'  search_history_folder: {search_history_folder}')
            logger.info(f'  error_results_folder: {error_results_folder}')
            logger.info(f'  annotation_folder: {annotation_folder}')
            logger.info(f'  actual_annotation_file: {actual_annotation_file}')
            
            # check folder and files
            
            if not sch_database_file.exists():
                logger.info('SCH Database file does not exists.')
                raise
            
            if not annotation_folder.exists():
                logger.info('Annotation folder does not exists.')
                raise
                
            # create output folder if they don't exists
            if not search_results_folder.exists():
                search_results_folder.mkdir(parents=True)
            if not search_history_folder.exists():
                search_history_folder.mkdir(parents=True)
            if not error_results_folder.exists():
                error_results_folder.mkdir(parents=True)
                
            config_params = {
                'sch_database_file': sch_database_file, 
                'search_results_folder': search_results_folder,
                'search_history_folder': search_history_folder,
                'error_results_folder': error_results_folder, 
                'annotation_folder': annotation_folder,
                'actual_annotation_file': actual_annotation_file}            
            logger.info('Success reading config file')
            
            return config_params
            
        except:
            logger.info('Error reading config file.')
            return False
    else:
        logger.info('Config file not found.')
        return False
    

if __name__ == '__main__':
    
    logging.basicConfig(filename='schwebsearch.log', level=logging.INFO)
    
    # command line args
    parser = argparse.ArgumentParser(description='Search Certified Products')    
    parser.add_argument('--total_itens_to_query', type=int, default=100, help='Quantidade de itens para pesquisar')
    parser.add_argument('--grace_period', type=int, default=180, help='Perído de carência para consultar produtos recém homologados')
    parser.add_argument('--verbose', type=bool, default=False, help='Imprimir os itens pesquisados')
    args = parser.parse_args()
    
    total_itens_to_query = args.total_itens_to_query
    grace_period = args.grace_period
    verbose = args.verbose
    
    now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    logger.info(f'Execution started:   {now}')
    logger.info('Actual arguments:')
    logger.info(f'  total_itens_to_query: {total_itens_to_query}')
    logger.info(f'  grace_period: {grace_period}')
    logger.info(f'  verbose: {verbose}')

    config = read_config_file()
    
    if not config:
        logger.info('Execution aborted.')
        exit(-1)
    else:
        sch_database_file = config['sch_database_file']
        search_results_folder = config['search_results_folder']
        search_history_folder = config['search_history_folder']
        error_results_folder = config['error_results_folder']
        annotation_folder = config['annotation_folder']
        actual_annotation_file = config['actual_annotation_file']
               
    # load sch database    
    df_sch = load_sch(sch_database_file,search_history_folder)
    # remove previously searched items
    df_sch = df_sch[df_sch['Última Pesquisa']==-1]
    
    # filter products certified before grace period
    if grace_period > 0:
        certification_date_limit = datetime.today().date() - timedelta(days=grace_period)
        certification_date_limit = certification_date_limit.strftime('%Y-%m-%d')
        df_sch = df_sch[df_sch['Data da Homologação']<=certification_date_limit]
    
    # new items to search
    items_to_google_search = df_sch['Número de Homologação'].iloc[:total_itens_to_query].to_list()
    items_to_bing_search = df_sch['Número de Homologação'].iloc[total_itens_to_query:total_itens_to_query*2].to_list()
    
    sch = SCHWebSearch(search_results_folder)
    
    for i, item in enumerate(items_to_google_search):
        response_code, file_name = sch.google_search(item)
        logger.info(f'Searching item (Google): {item}')
        logger.info(f'  Response code: {response_code}')
        logger.info(f'  File saved: {file_name}')
        if verbose:
            print(i, response_code, file_name)
        # 403 Client Error: Quota Exceeded for url
        # 429 Client Error: Too Many Requests for url 
        if response_code in [403, 429]:
            print('Exiting: search quota exceeded')
            logger.info('Exiting: Google search quota exceeded')
            break
    
    # disable Bing search: search quota exceeded until 2024-06-24
    for i, item in enumerate(items_to_bing_search):
        response_code, file_name = sch.bing_search(item)
        logger.info(f'Searching item (Bing): {item}')
        logger.info(f'  Response code: {response_code}')
        logger.info(f'  File saved: {file_name}')
        if verbose:
            print(i, response_code, file_name)
        # 403 Client Error: Quota Exceeded for url
        # 429 Client Error: Too Many Requests for url 
        if response_code in [403, 429]:
            print('Exiting: search quota exceeded')
            logger.info('Exiting: Bing search quota exceeded')
            break
           
    search_results_files = list(search_results_folder.glob('*.json'))
    
    logger.info('Creating annotation file')
    if verbose:
        print('Creating annotation file:')
        results = [parse_result_file(file,
                                     search_history_folder=search_history_folder,
                                     error_results_folder=error_results_folder) 
                   for file in tqdm(search_results_files)]
    else:
        results = [parse_result_file(file,
                                     search_history_folder=search_history_folder,
                                     error_results_folder=error_results_folder) 
                   for file in search_results_files]
    
    df_results = pd.DataFrame(results)
    
    if not df_results.empty:
        df_results = df_results[df_results['Situação']==1]
        status, annotation_file = save_annotation_file(df_results,annotation_folder,actual_annotation_file)
        if status==0:
            logger.info(f'Annotation file saved: {annotation_file}')
        elif status==1:
            logger.info('Annotation file not saved: empty results')    
        elif status==-1:
            logger.info('Error saving annotation file')
        else:
            logger.info('Unexpected status saving annotation file')
    else:
        logger.info('Annotation file not saved: empty results')

    now = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
    endline = '_'*54
    logger.info(f'Execution completed: {now}\n{endline}')
    
    if verbose:
        print('Pressione ENTER para sair...')
        _ = input()
    
    