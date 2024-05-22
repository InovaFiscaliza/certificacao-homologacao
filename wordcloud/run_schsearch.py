from schsearch import load_sch, SCHWebSearch

if __name__ == '__main__':
    df_sch = load_sch('datasets/produtos_certificados.zip',grace_period=180)
    df_sch = df_sch[df_sch['Última Pesquisa']==-1]
    items_to_search = df_sch['Número de Homologação'].head(10).to_list()
    
    sch = SCHWebSearch()
    
    for item in items_to_search:
        response_code, file_name = sch.google_search(item)
        print(response_code, file_name.name)
        
    print('Pressione ENTER para sair...')
    _ = input()
        