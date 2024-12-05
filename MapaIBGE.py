import pandas as pd

from GerenciarNeo4JDB import GerenciarNeo4JDB
from DadosGeoespacialBR import DadosGeoespacialBR


class MapaIBGE:
    def __init__(self):
        self.geodata = DadosGeoespacialBR()
        self.conexao_db = GerenciarNeo4JDB("bolt://localhost:7687", ("neo4j", "ianatelcom2024"))
        

    def criar_estados_capitais(self, mapabr):
        for nome,uf,capital,cod_ibge,regiao,cod_uf in DadosGeoespacialBR.estadosBRCSV():
            self.conexao_db.executeDB(f"MERGE (n:Estado {{  estado: '{nome}', uf: '{uf}', regiao: '{regiao}',cod_ibge: {cod_ibge} }}) ", 
                          database=mapabr)


    def criar_cidades(self, mapabr):
        for estado in DadosGeoespacialBR.estadosBRCSV():
            mun = self.geodata.municipios_estado(estado[1])
            for index, dados_mun in mun.iterrows():
                # uf: \"{dados_mun['SIGLA_UF']}\",
                self.conexao_db.executeDB(f"MERGE (n:Cidade {{ cod_ibge: {dados_mun['CD_MUN']}, uf: \'{dados_mun['SIGLA_UF']}\', municipio: \"{dados_mun['NM_MUN']}\",  area: \"{dados_mun['AREA_KM2']}\" }})", 
                                     database=mapabr)
                
    def interligar_estados(self, mapabr):
        
        for cidade1,cidade2 in DadosGeoespacialBR.fronteira_estadosBRCSV():
            print(f" MATCH (estado1:Estado {{cod_ibge: {cidade1} }}),(estado2:Estado {{cod_ibge: {cidade2} }} ) CREATE (estado2)-[:ESTADO_VIZINHO]->(estado1) , (estado1)-[:ESTADO_VIZINHO]->(estado2)")
            self.conexao_db.executeDB(f" MATCH (estado1:Estado {{cod_ibge: {cidade1} }}),(estado2:Estado {{cod_ibge: {cidade2} }} ) CREATE (estado2)-[:ESTADO_VIZINHO]->(estado1) , (estado1)-[:ESTADO_VIZINHO]->(estado2)", 
                                 database=mapabr)
            
    def interligar_cidades(self, mapabr):
        for uf in DadosGeoespacialBR.estadosBRCSV():
            arquivo_vizinhos = pd.read_csv(f'arestas_cidades_uf\\arestas_estado_{uf[1]}.csv')
            for index, cidades in arquivo_vizinhos.iterrows():
                self.conexao_db.executeDB(f" MATCH (city1:Cidade {{cod_ibge: {cidades['cidade1']} }}),(city2:Cidade {{cod_ibge: {cidades['cidade2']} }} ) CREATE (city2)-[:CIDADE_VIZINHA]->(city1) , (city1)-[:CIDADE_VIZINHA]->(city2)", database=mapabr)

    def cidades_vizinha(self, codigo_ibge, mapa):
        consulta = self.conexao_db.executeDB(f"MATCH (n:Cidade {{cod_ibge: {codigo_ibge} }})<-[r:CIDADE_VIZINHA]-(connected_node) RETURN connected_node)",
                                             database=mapa)
        return consulta

    def add_caracteristicas_cidades(self, mapadb, cod_ibge, propriedade, dado):
        return self.conexao_db.executeDB(f" MATCH (city:Cidade {{cod_ibge: {cod_ibge} }}) SET city.{propriedade} = {dado} RETURN  city; ", 
                                 database=mapadb)
            

    def interrelacionando_capital_estado(self, mapabr):
        return self.conexao_db.executeDB("MATCH (c:Cidade),(e:Estado) WHERE c.cod_ibge=e.cod_ibge CREATE (c)-[r1:LOCALIZADA_EM]->(e) , (e)-[r2:CONTEM_CAPITAL]->(c) RETURN r1, r2",database=mapabr)
   
    def adicionar_centroides_cidades(self, mapa):
        file_centroide = pd.read_csv('centroides_cidades_shp.csv', low_memory=False)
        for id,data_ibge in file_centroide.iterrows():
            self.add_caracteristicas_cidades( mapa,int(data_ibge['cod_ibge']), 'centroideCidade',
                                                      f'point({{ latitude: {data_ibge['latitude_x']}, longitude: {data_ibge['longitude_y']}, crs: \'wgs-84\' }})')
    def adicionar_populacao_cidade(self, mapa):
        populacaomuninipios_2024= pd.read_csv(f'estimativa_populacao_municipio_2024.csv', sep=';',low_memory=False, on_bad_lines='warn')

        for id,dados_ibge in populacaomuninipios_2024.iterrows():
            print(f" MATCH (city:Cidade {{cod_ibge: {dados_ibge['COD_MUNICIPIO']} }}) SET city.populacao2024 = {dados_ibge['POPULACAO_ESTIMADA']} ")
            self.conexao_db.executeDB(f" MATCH (city:Cidade {{cod_ibge: {dados_ibge['COD_MUNICIPIO']} }}) SET city.populacao2024 = {dados_ibge['POPULACAO_ESTIMADA']}  ", 
                                 mapa)
            
    def adicionar_populacao_estado(self, mapa):
        populacaoestados_2024= pd.read_csv(f'estimativa_populacao_estado_2024.csv', sep=';',low_memory=False, on_bad_lines='warn')

        for id,dados_ibge in populacaoestados_2024.iterrows():
            self.conexao_db.executeDB(f" MATCH (city:Estado {{estado: \'{dados_ibge['UF']}\' }}) SET city.populacao2024 = {dados_ibge['POPULACAO_ESTIMADA']}  ", 
                                 mapa)
            
    def adicionar_cetroides_estados(self, mapa):

        centroide_estado = pd.read_csv(f'[GEOJSON]mapa_estados_br\\extracao_estados_centroides.csv', sep=',',low_memory=False, on_bad_lines='warn')

        for id, centroide in centroide_estado.iterrows():
            self.conexao_db.executeDB(f" MATCH (city:Estado {{uf: \'{centroide['UF']}\' }}) SET city.centroide = {centroide['centroide']}  ", 
                                 mapa)
            print(f" MATCH (city:Estado {{uf: \'{centroide['UF']}\' }}) SET city.centroide = {centroide['centroide']} ")

    def adicionar_ibge_externo(self, mapabr):

        path = lambda uf: f'[IBGE]dados/original_{uf}.csv'
        
        print("ok")
        for mapa in DadosGeoespacialBR.estadosBRCSV():
            dados_ibge= pd.read_csv(path(mapa[1]), sep=';',low_memory=False, on_bad_lines='warn')

            columns = dados_ibge.columns
            for index,info in dados_ibge.iterrows():
                
                cod_ibge = info[columns[1]]
                if not pd.isna(info[8]):
                    coluna = str(columns[8])
                    valor = info[columns[8]]
                    print(f" MATCH (city:Cidade {{cod_ibge: {cod_ibge}, uf:{mapa[1]} }}) SET city.{coluna} = {valor} RETURN  city; ")
                    self.conexao_db.executeDB(f"MATCH (city:Cidade {{cod_ibge: {cod_ibge} }}) SET city.{coluna} = {valor}  ", database=mapabr)
                if not pd.isna(info[10]):
                    coluna = str(columns[10])
                    valor = info[columns[10]]
                    print(f" MATCH (city:Cidade {{cod_ibge: {cod_ibge}, uf:{mapa[1]} }}) SET city.{coluna} = {valor} RETURN  city; ")
                    self.conexao_db.executeDB(f"MATCH (city:Cidade {{cod_ibge: {cod_ibge} }}) SET city.{coluna} = {valor}  ", database=mapabr)
                if not pd.isna(info[11]):
                    coluna = str(columns[11])
                    valor = info[columns[11]]
                    print(f" MATCH (city:Cidade {{cod_ibge: {cod_ibge}, uf:{mapa[1]} }}) SET city.{coluna} = {valor} RETURN  city; ")
                    self.conexao_db.executeDB(f"MATCH (city:Cidade {{cod_ibge: {cod_ibge} }}) SET city.{coluna} = {valor}  ", database=mapabr)
                if not pd.isna(info[12]):
                    coluna = str(columns[12])
                    valor = info[columns[12]]
                    print(f" MATCH (city:Cidade {{cod_ibge: {cod_ibge}, uf:{mapa[1]} }}) SET city.{coluna} = {valor} RETURN  city; ")
                    self.conexao_db.executeDB(f"MATCH (city:Cidade {{cod_ibge: {cod_ibge} }}) SET city.{coluna} = {valor}  ", database=mapabr)

    def adicionar_ibge_externo_uf(self, mapabr):

    
        dados_ibge= pd.read_csv(f'[IBGE]dados/original_UF.csv', sep=';',low_memory=False, on_bad_lines='warn')

        columns = dados_ibge.columns
        for index,info in dados_ibge.iterrows():
            
            self.conexao_db.executeDB(f" MATCH (city:Estado {{estado: \'{info[columns[0]]}\' }}) SET city.{columns[5]} = {info[columns[5]]}", database=mapabr)
            self.conexao_db.executeDB(f" MATCH (city:Estado {{estado: \'{info[columns[0]]}\' }}) SET city.{columns[9]} = {info[columns[9]]}", database=mapabr)
            self.conexao_db.executeDB(f" MATCH (city:Estado {{estado: \'{info[columns[0]]}\' }}) SET city.{columns[10]} = {info[columns[10]]}", database=mapabr)
            self.conexao_db.executeDB(f" MATCH (city:Estado {{estado: \'{info[columns[0]]}\' }}) SET city.{columns[11]} = {info[columns[11]]}", database=mapabr)
            self.conexao_db.executeDB(f" MATCH (city:Estado {{estado: \'{info[columns[0]]}\' }}) SET city.{columns[11]} = {info[columns[11]]}", database=mapabr)



    def adicionar_dado_geometria(self, mapa):
        brasil = self.geodata.get_municipios_ibge_2022()
        for id,info in brasil.iterrows():
            area = str(info['geometry'])
            self.add_caracteristicas_cidades(mapa, str(info['CD_MUN']), 'geometry',f'\'{area}\'')

    def __del__(self):
        self.conexao_db.encerrar_sessao()

            
    def montarDB(self, mapa='mapaibge'):
        self.criar_estados_capitais(mapa)
        print('self.criar_estados_capitais(mapa)')
        self.criar_cidades(mapa)
        print('self.criar_cidades(mapa)')
        self.interligar_estados(mapa)
        print('self.interligar_estados(mapa)')
        self.interligar_cidades(mapa)
        print('self.interligar_cidades(mapa)')
        self.interrelacionando_capital_estado(mapa)
        print('self.interrelacionando_capital_estado(mapa)')
        self.adicionar_centroides_cidades(mapa)
        print("self.adicionar_centroides_cidades(mapa)")
        self.adicionar_populacao_cidade(mapa)
        print("adicionar_populacao_cidade(self, mapa)")
        self.adicionar_populacao_estado(mapa)
        print("self.adicionar_populacao_estado(mapa)")

        self.adicionar_ibge_externo(mapa)
        print('adicionar_dadosibge(self, mapa)')
        self.adicionar_ibge_externo_uf(mapa)
        print('adicionar_ibge_externo_uf(self, mapabr)')
        # self.adicionar_dado_geometria(mapa)
        # print("adicionar_dado_geometria(self, mapa)")

