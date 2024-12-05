
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from DadosGeoespacialBR import DadosGeoespacialBR
from MapaIBGE import MapaIBGE
from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB

#   ("Mato Grosso do Sul","MS", "Campo Grande","2401305",52),
#   ("Mato Grosso","MT", "Cuiabá", "5103403",51),
#   ("Goiás","GO", "Goiânia", "5208707",52),
#   ("Distrito Federal","DF", "Brasília", "5300108",53)]}



# sistema = MosaicoDB(uf='MS')
# sistema.montando_estrutura('MS','testecentrooeste')
# print(F'montando_estrutura(\'MS\',\'testecentrooeste\')')


# sistema = MosaicoDB(uf='MT')
# sistema.montando_estrutura('MT','testecentrooeste')
# print(F'montando_estrutura(\'MT\',\'testecentrooeste\')')

# sistema = MosaicoDB(uf='GO')
# sistema.montando_estrutura('GO','testecentrooeste')
# print(F'montando_estrutura(\'GO\',\'testecentrooeste\')')

# sistema = MosaicoDB(uf='DF')
# sistema.montando_estrutura('DF','testecentrooeste')
# print(F'montando_estrutura(\'DF\',\'testecentrooeste\')')

mapa = MapaIBGE()
mapa.montarDB(mapa='testecentrooeste')