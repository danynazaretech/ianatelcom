
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from MapaIBGE import MapaIBGE
from DadosGeoespacialBR import DadosGeoespacialBR

from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB


# sistema = MosaicoDB(uf='DF')
# sistema.montando_estrutura('DF','testedistritofederal')
# print(F'montando_estrutura(\'DF\',\'testedistritofederal\')')

sistema = MosaicoDB(uf='DF')
sistema.montando_estrutura('DF','testecentrooeste')
print(F'montando_estrutura(\'DF\',\'testecentrooeste\')')



# mapa = MapaIBGE()
# mapa.montarDB(mapa='testedistritofederal')