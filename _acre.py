
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from MapaIBGE import MapaIBGE
from DadosGeoespacialBR import DadosGeoespacialBR
from GerenciarDadosSMP import GerenciarDadosSMP
from ManipulandoDadosSMP import ManipulandoDadosSMP
from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB


sistema = MosaicoDB(uf='AC')
sistema.montando_estrutura('AC','testeacre')
print(F'montando_estrutura(\'AC\',\'testeacre\')')

mapa = MapaIBGE()
mapa.montarDB(mapa='testeacre')