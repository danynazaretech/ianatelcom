
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from DadosGeoespacialBR import DadosGeoespacialBR
from GerenciarDadosSMP import GerenciarDadosSMP
from ManipulandoDadosSMP import ManipulandoDadosSMP
from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB
from MapaIBGE import MapaIBGE


sistema = MosaicoDB(uf='RJ')
sistema.montando_estrutura('RJ','testeriodejaneiro')
print(F'montando_estrutura(\'RJ\',\'testeriodejaneiro\')')