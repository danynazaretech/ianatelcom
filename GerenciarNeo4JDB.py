from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

class GerenciarNeo4JDB:
    '''
        ------------------------------------------------
        - A classe faz a  manutenção do banco de dados -
        ------------------------------------------------
    '''
    def __init__(self, URI, AUTH):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        #Por favor, verificar a versão self.driver.execute_query(expressao)
        with self.driver.session() as session_db: 
            result = session_db.run("MATCH (n) RETURN n")
            print("Conexão Iniciada")
                
    def executeDB(self, expressao, database="neo4j"):
        with self.driver.session(database=database) as session_db:
            #Por favor, verificar a versão self.driver.execute_query(expressao)

            result = session_db.run(expressao)
            return result
    def get_banco_dados(self):
        return self.driver
        
    def encerrar_sessao(self):
        self.driver.close()

    def __del__(self):
        self.driver.close()
