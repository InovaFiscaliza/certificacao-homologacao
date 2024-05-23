import argparse

from schsearch import load_sch, SCHWebSearch

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Search Certified Products')    
    parser.add_argument('total_itens_to_query', type=int, default=10, help='Quantidade de itens para pesquisar')    
    parser.add_argument('--verbose', type=bool, default=False, help='Imprimir os itens pesquisados')
    args = parser.parse_args()
    
    total_itens_to_query = args.total_itens_to_query
    verbose = args.verbose
    
    df_sch = load_sch('datasets/produtos_certificados.zip',grace_period=180)
    df_sch = df_sch[df_sch['Última Pesquisa']==-1]
    items_to_search = df_sch['Número de Homologação'].head(total_itens_to_query).to_list()
    
    sch = SCHWebSearch()
    
    for i,item in enumerate(items_to_search):
        try:
            response_code, file_name = sch.google_search(item)
            if verbose:
                print(i, response_code, file_name.name)
        except:
            continue
            
    print('Pressione ENTER para sair...')
    _ = input()
    