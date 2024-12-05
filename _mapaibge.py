
import pandas as pd
from neo4j import GraphDatabase
import geopandas as gpd
from MapaIBGE import MapaIBGE
from DadosGeoespacialBR import DadosGeoespacialBR

from MosaicoDB import MosaicoDB
from GerenciarNeo4JDB import GerenciarNeo4JDB




mapa = MapaIBGE()
mapa.montarDB(mapa='mapaibge')