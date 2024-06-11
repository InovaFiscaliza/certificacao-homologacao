import argparse
import configparser
import pandas as pd
import os.path as osp

from datetime import datetime, timedelta
from pathlib import Path
from tqdm.auto import tqdm

from schwebsearch import load_sch, SCHWebSearch, parse_result_file, save_annotation_file

if __name__ == '__main__':
    
    # command line args
    parser = argparse.ArgumentParser(description='Search Certified Products')    
    parser.add_argument('--total_itens_to_query', type=int, default=100, help='Quantidade de itens para pesquisar')
    parser.add_argument('--grace_period', type=int, default=180, help='Perído de carência para consultar produtos recém homologados')
    parser.add_argument('--verbose', type=bool, default=False, help='Imprimir os itens pesquisados')
    args = parser.parse_args()
    
    total_itens_to_query = args.total_itens_to_query
    grace_period = args.grace_period
    verbose = args.verbose
    
    # read config file
    config = configparser.ConfigParser()
    if osp.exists('websearch_config.ini'):
        print('Reading config file ... ', end='')
        try:
            config.read('websearch_config.ini')
            sch_database_file = Path(config['SCHWEBSEARCH']['sch_database_file'])
            search_results_folder = Path(config['SCHWEBSEARCH']['search_results_folder'])
            parsed_results_folder = Path(config['SCHWEBSEARCH']['parsed_results_folder'])
            error_results_folder = Path(config['SCHWEBSEARCH']['error_results_folder'])
            annotation_folder = Path(config['SCHWEBSEARCH']['annotation_folder'])
            print('success.')
        except:
            print('config file is corrupted. Execution aborted.')
            exit(-1)
    else:
        print('Config file not found. Execution aborted.')
            
    # load sch database    
    df_sch = load_sch(sch_database_file,parsed_results_folder)    
    # remove searched items
    df_sch = df_sch[df_sch['Última Pesquisa']==-1]
    
    # filter products certifieds before grace period
    if grace_period > 0:
        certification_date_limit = datetime.today().date() - timedelta(days=grace_period)
        certification_date_limit = certification_date_limit.strftime('%Y-%m-%d')
        df_sch = df_sch[df_sch['Data da Homologação']<=certification_date_limit]
    
    # new items to search
    items_to_search = df_sch['Número de Homologação'].head(total_itens_to_query).to_list()
    
    sch = SCHWebSearch(search_results_folder)
    
    # for i, item in enumerate(items_to_search):
    #     response_code, file_name = sch.google_search(item)
    #     if verbose:
    #         print(i, response_code, file_name)
    #     # 403 Client Error: Quota Exceeded for url
    #     # 429 Client Error: Too Many Requests for url 
    #     if response_code in [403, 429]:
    #         print('Exiting: search quota exceeded')
    #         break
    
    # disable Bing search: search quota exceeded until 2024-06-24
    # for i, item in enumerate(items_to_search):
    #     response_code, file_name = sch.bing_search(item)
    #     if verbose:
    #         print(i, response_code, file_name)
    #     # 403 Client Error: Quota Exceeded for url
    #     # 429 Client Error: Too Many Requests for url 
    #     if response_code in [403, 429]:
    #         print('Exiting: search quota exceeded')
    #         break
           
    search_results_files = list(search_results_folder.glob('*.json'))
    
    if verbose:
        print('Creating annotation file:')
        results = [parse_result_file(file,
                                     parsed_results_folder=parsed_results_folder,
                                     error_results_folder=error_results_folder) 
                   for file in tqdm(search_results_files)]
    else:
        results = [parse_result_file(file,
                                     parsed_results_folder=parsed_results_folder,
                                     error_results_folder=error_results_folder) 
                   for file in search_results_files]
    
    df_results = pd.DataFrame(results)
    if not df_results.empty:
        df_results = df_results[df_results['Situação']==1]
        save_annotation_file(df_results,annotation_folder)

    if verbose:
        print('Pressione ENTER para sair...')
        _ = input()
    