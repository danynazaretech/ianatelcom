
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from DadosGeoespacialBR import DadosGeoespacialBR
from GerenciarDadosSMP import GerenciarDadosSMP
from ManipulandoDadosSMP import ManipulandoDadosSMP
from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB



sistema = MosaicoDB()
sistema.montando_estrutura('AC','testeacre')
print(F'montando_estrutura(\'AC\',\'testeacre\')')