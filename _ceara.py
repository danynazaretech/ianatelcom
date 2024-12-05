
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from DadosGeoespacialBR import DadosGeoespacialBR
from GerenciarDadosSMP import GerenciarDadosSMP
from ManipulandoDadosSMP import ManipulandoDadosSMP
from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB


from MapaIBGE import MapaIBGE
mapa = MapaIBGE()
mapa.montarDB(mapa='testeceara')

# sistema = MosaicoDB(uf='CE')
# sistema.montando_estrutura('CE','testeceara')
# print(F'montando_estrutura(\'CE\',\'testeceara\')')