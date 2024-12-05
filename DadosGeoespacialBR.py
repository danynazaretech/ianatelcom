import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import os


class DadosGeoespacialBR:
    '''
            ----------------------------------------------
            Métodos onde hospedam as operações básicas
            para a manipulação do mapa do brazil segundo
            os dados do IBGE de 2022
            ----------------------------------------------
    '''

    
    def __init__(self):
        self.geoBR = gpd.read_file('BR_Municipios_2022\\BR_Municipios_2022.shp')  

        self.arquivo_municipios = lambda uf: f"[GEOJSON]mapa_municipios_br/municipios_{uf}.geojson" 
        self.path = lambda uf: f"[GEOJSON]mapa_cidades_br/estado_{uf}"

    @staticmethod
    def estadosBRJSON():
        estados_br_json = {'Sul':[("Paraná","PR","Curitiba", "4106902",41 ),
                                       ("Santa Catarina","SC","Florianópolis", "4205407",43),
                                       ("Rio Grande do Sul","RS", "Porto Alegre", "4314902",43)],
              'Sudeste':[("Minas Gerais","MG","Belo Horizonte","3106200",31),
                         ("Espírito Santo","ES", "Vitória", "3205309",32),
                         ("Rio de Janeiro","RJ","Rio de Janeiro", "3304557",33),
                         ("São Paulo","SP", "São Paulo","3550308",35) ],
              'Norte':[
                       ("Rondônia","RO","Porto Velho","1100205",11),
                       ("Acre","AC","Rio Branco","1200401",12 ),
                       ("Amazonas","AM", "Manaus", "1302603",13),
                       ("Roraima","RR", "Boa Vista","2502151",14),
                       ("Pará","PA","Belém","2700805",15),
                       ("Amapá","AP", "Macapá","1600303",16),
                       ("Tocantins","TO","Palmas","1721000",17)],
              'Nordeste':[("Maranhão","MA", "São Luís","2111300",21),
                          ("Piauí","PI", "Teresina","2211001",22),
                          ("Ceará","CE", "Fortaleza", "2304400",23),
                          ("Rio Grande do Norte","RN", "Natal","2408102",24),
                          ("Paraíba","PB", "João Pessoa","2507507",25),
                          ("Pernambuco","PE", "Recife","2611606",26),
                          ("Alagoas","AL","Maceió", "2704302",27),
                          ("Sergipe","SE", "Aracaju", "2800308",28),
                          ("Bahia","BA", "Salvador", "2927408",29)
                          ],
              'Centro-Oeste':[
                              ("Mato Grosso do Sul","MS", "Campo Grande","2401305",52),
                              ("Mato Grosso","MT", "Cuiabá", "5103403",51),
                              ("Goiás","GO", "Goiânia", "5208707",52),
                              ("Distrito Federal","DF", "Brasília", "5300108",53)]}
        return estados_br_json
        
    @staticmethod
    def estadosBRCSV():
        estados_br_csv = [
            ("Acre","AC","Rio Branco","1200401" ,"Norte",12),
            ("Alagoas","AL","Maceió", "2704302","Nordeste",27),
            ("Amazonas","AM", "Manaus", "1302603","Norte",13),
            ("Amapá","AP", "Macapá","1600303","Norte",16),
            ("Bahia","BA", "Salvador", "2927408","Nordeste",29),
            ("Ceará","CE", "Fortaleza", "2304400","Nordeste",23),
            ("Distrito Federal","DF", "Brasília", "5300108","Centro-Oeste",53),
            ("Espírito Santo","ES", "Vitória", "3205309","Sudeste",32),
             ("Goiás","GO", "Goiânia", "5208707","Centro-Oeste",52),
             ("Maranhão","MA", "São Luís","2111300","Nordeste",21),
             ("Mato Grosso","MT", "Cuiabá", "5103403","Centro-Oeste",51),
             ("Mato Grosso do Sul","MS", "Campo Grande","2401305","Centro-Oeste",50),
             ("Minas Gerais","MG","Belo Horizonte","3106200","Sudeste",31),
             ("Pará","PA","Belém","2700805","Norte",15),
             ("Paraíba","PB", "João Pessoa","2507507","Nordeste",25),
             ("Paraná","PR","Curitiba", "4106902","Sul",41),
             ("Pernambuco","PE", "Recife","2611606","Nordeste",26),
             ("Piauí","PI", "Teresina","2211001","Nordeste",22),
             ("Rio de Janeiro","RJ","Rio de Janeiro", "3304557","Sudeste",33),
             ("Rio Grande do Norte","RN", "Natal","2408102","Nordeste",24),
             ("Rio Grande do Sul","RS", "Porto Alegre", "4314902","Sul",43),
             ("Rondônia","RO","Porto Velho","1100205","Norte",11),
             ("Roraima","RR", "Boa Vista","2502151","Norte",14),
             ("Santa Catarina","SC","Florianópolis", "4205407","Sul",42),
             ("São Paulo","SP", "São Paulo","3550308","Sudeste",35),
             ("Sergipe","SE", "Aracaju", "2800308","Nordeste",28),                  
             ("Tocantins","TO","Palmas","1721000","Norte",17)]
        return estados_br_csv

    @staticmethod
    def fronteira_estadosBRCSV():
        fronteiras_estados_br = [("4314902","4205407"), #RS-SC
                         ("4106902","4205407"), #SC-PR
                         ("4106902","3550308"), #PR-SP
                         ("4106902","2401305"), #PR-MS
                         ("3550308","2401305"), #SP-MS
                         ("3550308","3106200"), #SP-MG
                         ("3550308","3304557"), #SP-RJ
                         ("3304557","3106200"), #RJ-MG
                         ("3304557","3205309"), #RJ-ES
                         ("3205309","3106200"), #ES-MG
                         ("3205309","2927408"), #ES-BA
                         ("3106200","2401305"), #MG-MS
                         ("3106200","5208707"), #MG-GO
                         ("3106200","2927408"), #MG-BA
                         ("2401305","5103403"), #MS-MT
                         ("2401305","5208707"), #MS-GO
                         ("5208707","5103403"), #GO-MT
                         ("5208707","1721000"), #GO-TO
                         ("5208707","2927408"), #GO-BA
                         ("5103403","2502151"), #MT-RO
                         ("5103403","1302603"), #MT-AM
                         ("5103403","2700805"), #MT-PA
                         ("5103403","1721000"), #MT-TO
                         ("1100205","1302603"), #RO-AM
                         ("1100205","1200401"), #RO-AC
                         ("1200401","1302603"), #AC-AM
                         ("1302603","2502151"), #AM-RR
                         ("1302603","2700805"), #AM-PA
                         ("2502151","2700805"), #RR-PA
                         ("1600303","2700805"), #AP-PA
                         ("2700805","1721000"), #PA-TO
                         ("2700805","2111300"), #PA-MA
                         ("1721000","2111300"), #TO-MA
                         ("1721000","2927408"), #TO-BA
                         ("1721000","2211001"), #TO-PI
                         ("2111300","2211001"), #MA-PI
                         ("2211001","2304400"), #PI-CE
                         ("2211001","2611606"), #PI-PE
                         ("2211001","2927408"), #PI-BA
                         ("2927408","2800308"), #BA-SE
                         ("2927408","2704302"), #BA-AL
                         ("2927408","2611606"), #BA-PE
                         ("2800308","2704302"), #SE-AL
                         ("2704302","2611606"), #AL-PE
                         ("2611606","2507507"), #PE-PB
                         ("2507507","2304400"), #PB-CE
                         ("2507507","2408102"), #PB-RN
                         ("2408102","2304400"), #RN-CE
                         ("5300108","5208707")#DF-GO
                        ]
        return fronteiras_estados_br
        
    @staticmethod
    def capitaisBR():
        capitais = []
        for estados in DadosGeoespacialBR.estadosBRCSV():
            capitais.append(estados[2])
        return capitais
    
    def info(self):
        '''
            Principais dados do mapa BR_Municipios_2022.shp
            gdf.columns() - nomes das colunas
            gdf.crs() - sistema de coordenadas de referência 
            gdf.info() - mostra informações gerais sobre os dados
            gdf.head() - mostra as primeiras linhas do dataframe
            '''
        return self.geoBR.head()

    def get_municipios_ibge_2022(self):
        return self.geoBR 
    
    def faz_fronteira_cidades(self, municipio1, municipio2):
        m1 = self.geoBR[self.geoBR['CD_MUN'] ==  municipio1].geometry.iloc[0]
        m2 = self.geoBR[self.geoBR['CD_MUN'] ==  municipio2].geometry.iloc[0]
        return m1.touches(m2)

    def faz_fronteira_estados(self, estado1,estado2):
        return (estado1,estado2) in DadosGeoespacialBR.fronteira_estadosBRCSV()

    def municipios_estado(self, uf='MG'):
        return self.geoBR[self.geoBR['SIGLA_UF'] == uf.upper()]
        
    def aresta_cidades_estado(self, uf='MG'):
        fronteiras_arestas=[]
        estado = self.municipios_estado(uf.upper())
        for i, cidade1 in estado.iterrows():
            for j, cidade2 in estado.iterrows():
                if i < j: 
                    if self.faz_fronteira_cidades(cidade1['CD_MUN'],cidade2['CD_MUN']):
                        fronteiras_arestas.append((cidade1['CD_MUN'],cidade2['CD_MUN']))
        return fronteiras_arestas

    def desenhoMapaBR(self):
        # Melhorar essa função
        self.geoBR.plot(color='blue', edgecolor='black', alpha=0.5)

    def exportar_arestas_cidades_csv(self, lista, arquivo_salvar):
        #Exportar um arquivo de arestas de cidades vizinhas para csv
        dado_formatado = pd.DataFrame(lista, columns=['cidade1', 'cidade2'])
        dado_formatado.to_csv(arquivo_salvar, index=False)


    def gerar_arestas_cidades_vizinhas_uf(self):
        # Algoritmo responsável por gerar as arestas das cidades vizinhas de um estado
        for i in DadosGeoespacialBR.estadosBRCSV():
            lista = self.aresta_cidades_estado(i[1])
            self.exportar_arestas_cidades_csv(lista, f'arestas_cidades_uf\\arestas_estado_{i[1]}.csv')
            
    def get_centroide(self, cod_ibge): 
        self.geoBR.to_crs("EPSG:4326")
        return self.geoBR[self.geoBR['CD_MUN']== cod_ibge ].geometry.centroid

    def extraindo_centroides_cidades(self):
        centroide_csv = {'cod_ibge':[], 'latitude_x':[], 'longitude_y':[]}
        
        for id,data_ibge in self.geoBR.iterrows():
            centroide = self.get_centroide(str(data_ibge['CD_MUN']))
            centroide_csv['cod_ibge'].append(data_ibge['CD_MUN'])
            centroide_csv['latitude_x'].append(centroide.x.iloc[0].round(6))
            centroide_csv['longitude_y'].append(centroide.y.iloc[0].round(6))
        
        extracao_centroides = pd.DataFrame(centroide_csv)
        extracao_centroides.to_csv(f'centroides_cidades_shp.csv', index='False')

    def centroide_estados(self):
        estados = gpd.read_file("[GEOJSON]mapa_estados_br/estados.geojson")
        estados_centroide_csv = {'estados':[],'UF':[], 'centroide':[], 'geometry':[]}
        
        for i, info in estados.iterrows():
            centroide = info['geometry'].centroid
            estados_centroide_csv['estados'].append(info['Estado'])
            estados_centroide_csv['UF'].append(info['UF'])
            estados_centroide_csv['centroide'].append(f'point({{x: {centroide.x}, y:{centroide.y} }})')
            estados_centroide_csv['geometry'].append(centroide)
        extracao_centroides = pd.DataFrame(estados_centroide_csv)
        extracao_centroides.to_csv(f'[GEOJSON]mapa_estados_br\\extracao_estados_centroides.csv', index='False')

    def centroide_regioes(self):
        regioes = gpd.read_file("[GEOJSON]mapa_regioes_br/regioes.geojson")

        regiao_centroide_csv = {'regiao':[], 'centroide':[], 'geometry':[]}
        
        for i, info in regioes.iterrows():
            centroide = info['geometry'].centroid
            regiao_centroide_csv['regiao'].append(info['Região'])
            regiao_centroide_csv['centroide'].append(f'point({{x: {centroide.x}, y:{centroide.y} }})')
            regiao_centroide_csv['geometry'].append(centroide)
            
        extracao_centroides = pd.DataFrame(regiao_centroide_csv)
        extracao_centroides.to_csv(f'[GEOJSON]mapa_regioes_br\\extracao_regiao_centroides.csv', index='False')
        
    def extraindo_geojson_cidades(self):
        self.arquivo_municipios = lambda uf: f"[GEOJSON]mapa_municipios_br/municipios_{uf}.geojson" 
        
        for info_uf in DadosGeoespacialBR().estadosBRCSV():
            estado = self.municipios_estado(uf=info_uf[1])
            estado.to_file(self.arquivo_municipios(info_uf[1]), driver="GeoJSON")
                

    
    def extraindo_geojson_estados(self):
        # Definindo as colunas e o sistema de coordenadas (CRS)
        estados = gpd.GeoDataFrame(columns=['Estado','UF','Capital','Código Capital','Região','Código UF','geometry'], crs="EPSG:4326") 
                # EPSG:4326 para coordenadas WGS84
        
        for info in DadosGeoespacialBR.estadosBRCSV():
            dados = self.municipios_estado(uf=info[1])
            interseccao = dados.geometry.unary_union
            dados = {'Estado':[info[0]], 
                             'UF':[info[1]], 
                             'Capital':[info[2]], 
                             'Código Capital':[info[3]], 
                             'Região':[info[4]], 
                             'Código UF':[info[5]],
                             'geometry': [interseccao]}
            new_line = gpd.GeoDataFrame(dados, crs="EPSG:4326")
            estados = pd.concat([estados,new_line])
        estados.to_file("[GEOJSON]mapa_estados_br/estados.geojson", driver='GeoJSON')

    def extraindo_geojson_regioes(self):
        estados_geojson = gpd.read_file("[GEOJSON]mapa_estados_br/estados.geojson")
        regioes_gpd = gpd.GeoDataFrame(columns=['Região','geometry'], crs="EPSG:4326") 

        for regioes in DadosGeoespacialBR.estadosBRJSON():
            interseccao = estados_geojson[estados_geojson['Região']==regioes].geometry.unary_union
            new_line = gpd.GeoDataFrame([{'Região':regioes,'geometry':interseccao}], crs="EPSG:4326")
            regioes_gpd = pd.concat([regioes_gpd,new_line],ignore_index=True)
        regioes_gpd.to_file("[GEOJSON]mapa_regioes_br/regioes.geojson", driver='GeoJSON')


    def carregar_arquivo_cidade_geojson(self, estado, cidade):
        geojson_file = self.arquivo_municipio(estado, cidade)

        return gpd.read_file(geojson_file)

    def carregar_arquivo_estado_geojson(self, estado):
        geojson_file = self.arquivo_estado(estado)

        return gpd.read_file(geojson_file)

    def calculo_centroid_preciso(self,mapa):
        proj = mapa.to_crs(epsg=3395)
        centroide = proj.geometry.centroid
        return centroide.to_crs(epsg=4326)