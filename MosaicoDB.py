import pandas as pd
import re
import csv
from GerenciarNeo4JDB import GerenciarNeo4JDB
from DadosGeoespacialBR import DadosGeoespacialBR


class MosaicoDB:

    def __init__(self, uf='AC', separador=','):
       self.estados_uf = uf
       self.estacoes_smp = pd.read_csv(f'[ANATEL]dados_mosaico_antenas/licenciamento_{self.estados_uf}.csv', sep=',',low_memory=False, on_bad_lines='warn')
       
       self.conn = GerenciarNeo4JDB("bolt://localhost:7687", ("neo4j", "ianatelcom2024"))

       
       self.pastas_database = lambda uf: [f'[DATABASE]Antenas/database_antenas_{uf}.csv', 
                              f'[DATABASE]EstacaoRadioBase/database_estacaoradiobase_{uf}.csv',
                              f'[DATABASE]ErbAntenas/database_erb_antena_{uf}.csv',
                              f'[DATABASE]Tecnologias/database_tecnologia_{uf}.csv', 
                              f'[DATABASE]AntenaTecnologias/database_tecologia_antena_{uf}.csv',
                              f'[DATABASE]DownloadUpload/database_download_upload_{uf}.csv',
                              f'[DATABASE]Infraestrutura/database_infraestrutura_{uf}.csv',
                              f'[DATABASE]AntenaInfra/database_antena_infra_{uf}.csv',
                              f'[DATABASE]AntenaEquipamento/database_antena_equipamento_{uf}.csv',
                              f'[DATABASE]EquipamentoModularizacao/database_equipamento_modularizacao_{uf}.csv']


    def get_estacoes(self):
        return self.estacoes_smp

    def set_estado_estacoes(self,uf):
        self.estacoes_smp = pd.read_csv(f'[ANATEL]dados_mosaico_antenas/licenciamento_{uf}.csv', sep=',',low_memory=False, on_bad_lines='warn')
    @staticmethod
    def contagem_dados_estacoes(dados_smp):
        print(f'|----------Campo----------|----------Dados Diferentes----------|----------Celulas Vazias----------|')
        print(f'|-------------------------------------------------------------------------------------------------|')
        for info in dados_smp.columns:
            print(f'|----------{info}:----------{len(dados_smp[info].unique())}----------|----------{dados_smp[info].isna().sum()}----------|')
    
    
    @staticmethod
    def estrutura_dados_mosaico():
        return {'Status.state':[True,'Sem Uso'], 
        'NomeEntidade':[True,'EstacaoRadioBase'], 
        'NumFistel':[True,'EstacaoRadioBase'], 
        'NumServico':[True,'Sem Uso'], 
        'NumAto':[],#DOCUMENTO DE USO DE RADIO FREQUENCIA
        'NumEstacao':[True,'EstacaoRadioBase'], 
        'EnderecoEstacao':[True,'Sem Uso'], 
        'EndComplemento':[True,'EstacaoRadioBase'], 
        'SiglaUf':[True,'Sem Uso'],
        'CodMunicipio':[True,'EstacaoRadioBase'], 
        'DesignacaoEmissao':[], 
        'Tecnologia':[True,'Tecnologia'], 
        'tipoTecnologia':[True,'Tecnologia'],
        'meioAcesso':[True,'Sem Uso'], 
        'FreqTxMHz':[], 
        'FreqRxMHz':[], 
        'Azimute':[True,'Antena'],
        'CodTipoClasseEstacao':[True,'Sem Uso'], 
        'ClassInfraFisica':[True,'Infraestrutura'],
        'CompartilhamentoInfraFisica':[True, 'RelacionamentoAntenaInfra'], 
        'CodTipoAntena':[True,'Antena'],
        'CodEquipamentoAntena':[True,'Antena'],
        'GanhoAntena':[True,'Antena'], 
        'FrenteCostaAntena':[True,'Antena'], 
        'AnguloMeiaPotenciaAntena':[True,'Antena'],
        'AnguloElevacao':[True,'Antena'],
        'Polarizacao':[True,'Antena'],
        'AlturaAntena':[True,'Antena'],
        'CodEquipamentoTransmissor':[True, 'Equipamento'], 
        'PotenciaTransmissorWatts':[True, 'RepacionameentoAntenaEquipamento'], 
        'Latitude':[True,'EstacaoRadioBase'],
        'Longitude':[True,'EstacaoRadioBase'], 
        'CodDebitoTFI':[], 
        'DataLicenciamento':[True,'EstacaoRadioBase','ADC'],
        'DataPrimeiroLicenciamento':[True,'EstacaoRadioBase','ADC'],
        'NumRede':[], 
        '_id':[True,'Antena'], 
        'DataValidade':[True,'EstacaoRadioBase','ADC'],
        'NumFistelAssociado':[], 
        'NomeEntidadeAssociado':[],
        'Municipio.NomeMunicipio':[True,'Sem Uso']}

    @staticmethod
    def converter_decimal(numero,limiar):
        numero_decimal = re.sub(r'\,', '.', str(numero))
        numero_decimal = re.sub(r'\.', '', str(numero_decimal))
        return float(numero_decimal[:2] + '.' + numero_decimal[2:]) if float(numero_decimal)>limiar else float(numero)
    
    @staticmethod
    def remover_pontos(numero):
        partes = re.split(r'(\.)', numero, maxsplit=1)
        if len(partes) > 1:
            partes[2] = partes[2].replace('.', '')
            numero = ''.join(partes)
        return numero
        
    @staticmethod
    def formatando_padrao_numerico(numero):
        numero = str(numero)
        if re.search(r'[a-zA-Z]', numero) is not None:
            return 0.0
        numero = numero.replace(',','.')
        numero = MosaicoDB.remover_pontos(numero)
        isnumero = lambda n: bool(re.match(r'^[+-]?(\d+(\.\d*)?|\.\d+)$', n))
        if(isnumero(numero)):
            return numero

    @staticmethod
    def alterar_caracteres_indevidos(self,endereco):
        if endereco[0:3].upper() == 'AV ' or endereco[0:3].upper() == 'AV.':
            return 'AVENIDA'+ endereco[3:]
        elif  endereco[0:2].upper()=='R ' or  endereco[0:2].upper()=='R.':
            return 'RUA' + endereco[2:]
        elif  endereco[0:4].upper()=='ROD ' or  endereco[0:4].upper()=='ROD.':
            return 'RODOVIA' + endereco[4:]
        elif  endereco[0:4].upper()=='EST. ' or  endereco[0:4].upper()=='EST':
            return 'ESTRADA' + endereco[4:]
        
        return endereco
    
    def padronizando_coordenadas(self,lat,long):
        if(int(lat)< -100):
            lat = str(int(lat))
            lat = lat[0:3] + '.' + lat[3:] if len(lat)>=6 else lat[0:2] + '.' + lat[2:]
        if(int(long)< -100):
            long = str(int(long))
            long = long[0:3] + '.' + long[3:] if len(long)>=6 else long[0:2] + '.' + long[2:]
        return lat,long

    def adicionando_estacoes(self,uf,mapabr):
        clausulas = ''
        add_lote_inicial = '''UNWIND [''' 
        add_lote_final = '''] AS dado CREATE  (n:EstacaoRadioBase { num_estacao:dado.num_estacao, fistel:dado.fistel, 
                                                   nome_entidade:dado.nome_entidade, cod_ibge:dado.cod_ibge,
                                                   cod_debito_tfi:dado.cod_debito_tfi, cod_classe_estacao: dado.cod_classe_estacao,
                                                   latitude:dado.latitude, longitude:dado.longitude});'''
        lote = 0

        add = []
        dados_antenas = self.estacoes_smp['NumEstacao'].unique().tolist()
        for estacao_erb in dados_antenas:
            lote+=1
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb]
            erb=f'num_estacao:{estacao_erb},'

            if not pd.isna(estacao['NumFistel'].iloc[0]):
                erb += f"fistel:{estacao['NumFistel'].iloc[0]},"
            # if not pd.isna(estacao['NumFistelAssociado'].iloc[0]):
            #     erb += f"fistel_associado:{estacao['NumFistelAssociado'].iloc[0]},"
            if not pd.isna(estacao['NomeEntidade'].iloc[0]):
                erb+= f'nome_entidade:\'{estacao['NomeEntidade'].iloc[0]}\','
            # if not pd.isna(estacao['NomeEntidadeAssociado'].iloc[0]):
            #     erb+= f'nome_entidade_associado:\'{estacao['NomeEntidadeAssociado'].iloc[0]}\','
            if not pd.isna(estacao['SiglaUf'].iloc[0]):
                erb+= f'UF:\'{estacao['SiglaUf'].iloc[0]}\','
            if not pd.isna(estacao['CodMunicipio'].iloc[0]):
                erb+= f'cod_ibge:{estacao['CodMunicipio'].iloc[0]},'
            if not pd.isna(estacao['CodDebitoTFI'].iloc[0]):
                erb+= f'cod_debito_tfi: \'{estacao['CodDebitoTFI'].iloc[0]}\','
            if not pd.isna(estacao['CodTipoClasseEstacao'].iloc[0]):
                erb+= f'cod_classe_estacao: \'{estacao['CodTipoClasseEstacao'].iloc[0]}\' ,'
            # if not pd.isna(estacao['EnderecoEstacao'].iloc[0]):
            #     complemento = re.sub(r'[^a-zA-Z0-9\s]', ' ', estacao['EnderecoEstacao'].iloc[0])
            #     erb += f'Endereco:\'{complemento}\',' 
            # if not pd.isna(estacao['EndComplemento'].iloc[0]):
            #     complemento = re.sub(r'[^a-zA-Z0-9\s]', ' ', estacao['EndComplemento'].iloc[0])
            #     erb += f'complemento:\'{complemento}\',' 

            
            latitude =  self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb]['Latitude'].iloc[0]
            longitude =  self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb]['Longitude'].iloc[0]
            latitude, longitude = self.padronizando_coordenadas(latitude,longitude)

            erb += f'latitude:{latitude},longitude:{longitude}'

            if(lote<=100):
                clausulas+=f'{{ {erb} }},'
                
                if(lote==100):
                    # print(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
                    self.conn.executeDB(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}',database=mapabr)
                    # add.append(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
                    clausulas=''
                
                    # print(f'entra aqui+{lote}')
                    lote = 0


       
        if(lote!=0):

            # print(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
            self.conn.executeDB(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}',database=mapabr)
            # add.append(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
            
            #print(f'CREATE (n:EstacaoRadioBase {{ {erb} }})')
            # self.conn.executeDB(f'CREATE (n:EstacaoRadioBase {{ {erb} }})',database=mapabr)
            #add.append(f'CREATE (n{estacao_erb}:EstacaoRadioBase {{ {erb} }});' )
        
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]EstacaoRadioBase/database_estacaoradiobase_{uf}.cypher',sep='\t',index=False,header=False)
        
    def adicionando_antenas(self, uf,mapabr):
        clausulas = ''
        add_lote_inicial = '''UNWIND [''' 
       
        add_lote_final=''' ] AS dado CREATE (n:Antena { Azimute: dado.Azimute, CodTipoAntena: dado.CodTipoAntena,CodEquipamentoAntena: dado.CodEquipamentoAntena,
                    GanhoAntena: dado.GanhoAntena,FrenteCostaAntena: dado.FrenteCostaAntena, AnguloMeiaPotenciaAntena: dado.AnguloMeiaPotenciaAntena,
                    AnguloElevacao: dado.AnguloElevacao, Polarizacao: dado.Polarizacao, AlturaAntena: dado.AlturaAntena, NumAto: dado.NumAto,
                    DataPrimeiroLicenciamento: dado.DataPrimeiroLicenciamento, DataLicenciamento: dado.DataLicenciamento, 
                    DataValidade: dado.DataValidade,  UID: dado.UID } );'''
        lote = 0
        antenas = ['Azimute',
                   'CodTipoAntena',
            'CodEquipamentoAntena',
            'GanhoAntena', 
            'FrenteCostaAntena', 
            'AnguloMeiaPotenciaAntena',
            'AnguloElevacao',
            'Polarizacao',
            'AlturaAntena',
            'NumAto',
            'DataPrimeiroLicenciamento','DataLicenciamento','DataValidade',
            '_id'] 

    
        add = []
        i=0
        dados_antenas = self.estacoes_smp['NumEstacao'].unique().tolist()
 
        for estacao_erb in dados_antenas:
            nos_antenas =  self.estacoes_smp[ self.estacoes_smp['NumEstacao']==estacao_erb].drop_duplicates(subset=antenas)
            #campo_vazio = lambda x,valor: x if not x.isna() else valor
            
            for id,info in nos_antenas.iterrows():
                i+=1
                if(pd.isna(info['_id'])):
                    continue
                adicionar_db =  f""
                for var in antenas[0:13]:
                    if var in ['CodTipoAntena','FrenteCostaAntena','AnguloMeiaPotenciaAntena']:
                        if(pd.isna(info[var])):
                            adicionar_db+= f' Irregularidades: \'Sim\', ' 
                            break
                    if(not pd.isna(info[var])):
                        if var in ['NumAto','DataPrimeiroLicenciamento','DataLicenciamento','DataValidade']:
                            adicionar_db += f"{var}: \'{str(info[var])}\',"
                        elif var in [ 'CodTipoAntena','CodEquipamentoAntena','Polarizacao']:
                            
                            tira_char_especial = re.sub(r"[^a-zA-Z0-9\s]", '',str(info[var])) 
                            adicionar_db += f"{var}: \'{tira_char_especial}\',"
                        else:
                            n =  MosaicoDB.formatando_padrao_numerico(info[var]) 
                            adicionar_db += f"{var}: { n if n is not None else 0},"


                clausulas+=f'{{ {adicionar_db}  UID:\'{info['_id']}\' }},'
            
            # print(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
            self.conn.executeDB(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}',database=mapabr)
            clausulas=''
            
                # print(f'entra aqui+{lote}')
                # print(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')

                #add.append([f'CREATE (n{i}:Antena {{ {adicionar_db}  UID:\'{info['_id']}\' }} );'])
                #print(f'CREATE (n:Antena {{ {adicionar_db}  UID:\'{info['_id']}\' }} )')

                
                # self.conn.executeDB(f'CREATE (n:Antena {{ {adicionar_db}  UID:\'{info['_id']}\' }} )', database=mapabr)
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]Antenas/database_antenas_{uf}.cypher',sep='\t',index=False,header=False)

        return add
    
    def conectando_erb_antena(self, uf,mapabr):
        query = """
                UNWIND relationships AS rel
                MATCH (ant:Antena { UID: rel.uid}), (erb:EstacaoRadioBase  { num_estacao: rel.num_estacao }) 
                CREATE (erb)-[op:OPERA_ANTENA ]->(ant) 
                with op, rel
                where rel.properties.fistel_associada is not null  and rel.properties.entidade_associada is not null
                SET op += rel.properties            

                """
       
        relationships = ''
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        i=0

        for erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==erb]
            for id,info in estacao.iterrows():

                associado1 = f' fistel_associada: \'{info['NumFistelAssociado']}\' ' if not pd.isna(info['NumFistelAssociado']) else ''
                associado2 = f' entidade_associada: \'{info['NomeEntidadeAssociado']}\' ' if not pd.isna(info['NomeEntidadeAssociado']) else ''
                if(associado1=='' and associado2=='') :
                    associado = f''
                else:
                    associado = f' {associado1} , {associado2} '
                # cod_neo4j = f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (erb{i}:EstacaoRadioBase  {{ num_estacao: {info['NumEstacao']} }}) CREATE (erb{i})-[:OPERA_ANTENA  {associado}  ]->(ant{i}); """
                propriedades = f", properties: {{ {associado} }}" if associado!='' else ''
                relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']} {propriedades} }},"""
                # print(cod_neo4j)
                # i+=1
            self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}',database=mapabr)
            # with self.conn.get_banco_dados().session() as session_db:
            #     print(f'with [ {relationships[:-1]} ] as relationships  {query}')
            #     session_db.run(f'with [ {relationships[:-1]} ] as relationships  {query}')
            relationships=  ''
            
                
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]ErbAntenas/database_erb_antena_{uf}.cypher',sep='\t',index=False,header=False)
            
    def adicionando_tecnologias(self, uf,mapabr):
        clausulas = ''
        add_lote_inicial = '''UNWIND [''' 
       
        add_lote_final=''' ] AS dado CREATE (n:TecnologiaComunicacao { num_estacao: dado.num_estacao, tecnologia: dado.tecnologia , tipo_tecnologia:dado.tipo_tecnologia}  );'''
        add = []
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()

        for estacao_erb in num_estacao:
            nos_tecnologias =  self.estacoes_smp[ self.estacoes_smp['NumEstacao']==estacao_erb].drop_duplicates(subset=['Tecnologia', 'tipoTecnologia'])
            nos_tecnologias = nos_tecnologias[['Tecnologia', 'tipoTecnologia']].values.tolist()
            
            
            for info in nos_tecnologias:
                
                if pd.isna(info[1]):
                    clausulas += f'{{ num_estacao: {estacao_erb} ,tecnologia: \'{info[0]}\' }},'
                elif not pd.isna(info[1]):
                    clausulas += f'{{ num_estacao: {estacao_erb}, tecnologia: \'{info[0]}\',  tipo_tecnologia: \'{info[1]}\' }},'
                
                #add.append([concatenado_db])
            self.conn.executeDB(add_lote_inicial + clausulas[:-1] + add_lote_final, database=mapabr)
            clausulas = ''
        #df = pd.DataFrame(add, columns=['Nome'])
        #df.to_csv(f'[DATABASE]Tecnologias/database_tecnologia{uf}.cypher',sep='\t',index=False,header=False)
   
    def conectando_antena_tecnologia(self, uf, mapabr):
        query = """
               UNWIND relationships AS rel
               MATCH (ant:Antena { UID: rel.UID }), 
               (tec:TecnologiaComunicacao { num_estacao: rel.num_estacao, tecnologia: rel.tecnologia }) 
               WHERE rel.tipo_tecnologia IS NULL OR tec.tipo_tecnologia = rel.tipo_tecnologia
               CREATE (ant)-[:OPERA_SOB]->(tec);
               
                """
        add=[]
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        relationships=''
        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb]
            i = 0
            for id,info in estacao.iterrows():
                
                consulta_conecta = ''
                if(not pd.isna(info['tipoTecnologia'])):
                    consulta_conecta = f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), 
                    (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\',tipo_tecnologia:\'{info['tipoTecnologia']}\' }}) 
                    CREATE (ant{i})-[:OPERA_SOB]->(tec{i});"""
                    relationships += f'{{ UID: \'{info['_id']}\' ,  num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\', tipo_tecnologia:\'{info['tipoTecnologia']}\'}},'
                else:
                    consulta_conecta=f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\' }}) CREATE (ant{i})-[:OPERA_SOB]->(tec{i});"""
                    relationships += f'{{ UID: \'{info['_id']}\' ,  num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\'}},'
                # add.append(consulta_conecta)
                # print(consulta_conecta)
            # print(f'with [ {relationships[:-1]} ] as relationships  {query}')
            self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}',database=mapabr)
            relationships=  ''

        self.conn.executeDB(f'MATCH (tec:TecnologiaComunicacao) REMOVE tec.num_estacao', database=mapabr)
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]AntenaTecnologias/database_tecologia_antena_{uf}.cypher',sep='\t',index=False,header=False)

    def conectado_download_upload(self,uf, mapabr):

        query = """
                UNWIND relationships AS rel
                MATCH (ant:Antena { UID: rel.uid}), (erb:EstacaoRadioBase  { num_estacao: rel.num_estacao }) 
                CREATE (ant)-[op:OPERA_ANTENA ]->(erb),
                with op, rel
                where rel.properties.fistel_associada is not null  and rel.properties.entidade_associada is not null
                SET op += rel.properties            

                """
        query = """
               UNWIND relationships AS rel
               MATCH (ant:Antena { UID: rel.uid })
                (tec:TecnologiaComunicacao {num_estacao: rel.estacao, tecnologia: rel.tecnologia})
                WHERE rel.tipo_tecnologia IS NULL OR tec.tipo_tecnologia = rel.tipo_tecnologia
                CREATE (ant)-[tx:TAXA_UPLOAD ]->(tec),
                    (tec)-[rx:TAXA_DOWNLOAD ]->(ant);
                with tx,rx,rel
                where rel.properties.frequencia_rx  is not null  
                SET rx += rel.properties.frequencia_rx  
                with tx,rel
                where rel.properties.frequencia_tx  is not null  
                SET tx += rel.properties.frequencia_tx  

            """   
        add=[]
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        i=0
        relationships=''
        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 
            #filtrando_tecnologias= estacao.drop_duplicates(subset=['FreqTxMHz', 'FreqRxMHz'])[['FreqTxMHz', 'FreqRxMHz']].values
            tx_rx=''
            #ADICIONA TECNOLOGIA
            for id,info in estacao.iterrows():
                tipoTecnologia = '' if pd.isna(info['tipoTecnologia']) else f'tipoTecnologia: \'{info['tipoTecnologia']}\','
                
                
                if(not pd.isna(info['FreqTxMHz'])):
                    tx_rx += f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\' }})  CREATE (ant{i})-[:TAXA_UPLOAD {{ frequencia_tx: {info['FreqTxMHz']} }} ]->(tec{i});"""
                    # add.append([tx_rx])
                    # self.conn.executeDB(tx_rx,database=mapabr)
                    relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']}, {tipoTecnologia} properties: {{ frequencia_tx: {info['FreqTxMHz']} }} }},"""
                if(not pd.isna(info['FreqRxMHz'])):
                    tx_rx += f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\'  }}) CREATE (tec{i})-[:TAXA_DOWNLOAD {{ frequencia_rx: {info['FreqRxMHz']} }}]->(ant{i});"""
                    # add.append([tx_rx])
                    # self.conn.executeDB(tx_rx,database=mapabr)
                    relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']}, {tipoTecnologia} properties: {{ frequencia_rx: {info['FreqRxMHz']} }} }},"""

                i+=1
            # print(f'with [ {relationships[:-1]} ] as relationships  {query}')
            # print(tx_rx)
            self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}',database=mapabr)

           
            relationships=''
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]DownloadUpload/database_download_upload_{uf}.cypher',sep='\t',index=False,header=False)
    
    def conectado_upload(self,uf, mapabr):


            query = """
                UNWIND relationships AS rel
                MATCH (ant:Antena { UID: rel.uid }),
                    (tec:TecnologiaComunicacao {num_estacao: rel.num_estacao, tecnologia: rel.tecnologia})
                    WHERE rel.tipo_tecnologia IS NULL OR tec.tipo_tecnologia = rel.tipo_tecnologia
                    CREATE (ant)-[op:TAXA_UPLOAD ]->(tec)
                    with op,rel
                    where rel.properties.frequencia_tx  is not null 
                    SET op += rel.properties    
                """   
            add=[]
            num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
            i=0
            relationships=''
            for estacao_erb in num_estacao:
                estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 
                #filtrando_tecnologias= estacao.drop_duplicates(subset=['FreqTxMHz', 'FreqRxMHz'])[['FreqTxMHz', 'FreqRxMHz']].values
                tx_rx=''
                #ADICIONA TECNOLOGIA
                for id,info in estacao.iterrows():
                    tipoTecnologia = '' if pd.isna(info['tipoTecnologia']) else f'tipo_tecnologia: \'{info['tipoTecnologia']}\','
                    
                    
                    if(not pd.isna(info['FreqTxMHz'])):
                        tx_rx += f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\' }})  CREATE (ant{i})-[:TAXA_UPLOAD {{ frequencia_tx: {info['FreqTxMHz']} }} ]->(tec{i});"""
                        # add.append([tx_rx])
                        # self.conn.executeDB(tx_rx,database=mapabr)
                        relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']}, tecnologia: \'{info['Tecnologia']}\', {tipoTecnologia} properties: {{ frequencia_tx: {info['FreqTxMHz']} }} }},"""
                    # if(not pd.isna(info['FreqRxMHz'])):
                    #     tx_rx += f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\'  }}) CREATE (tec{i})-[:TAXA_DOWNLOAD {{ frequencia_rx: {info['FreqRxMHz']} }}]->(ant{i});"""
                    #     # add.append([tx_rx])
                    #     # self.conn.executeDB(tx_rx,database=mapabr)
                    #     relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']}, tecnologia: \'{info['Tecnologia']}\', {tipoTecnologia} properties: {{ frequencia_rx: {info['FreqRxMHz']} }} }},"""

                    i+=1
                # print(f'with [ {relationships[:-1]} ] as relationships  {query}')
                # print(tx_rx)
                self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}',database=mapabr)

            
                relationships=''
            # df = pd.DataFrame(add, columns=['Nome'])
            # df.to_csv(f'[DATABASE]DownloadUpload/database_download_upload_{uf}.cypher',sep='\t',index=False,header=False)
  
    def conectado_download(self,uf, mapabr):


        query = """
               UNWIND relationships AS rel
               MATCH (ant:Antena { UID: rel.uid }),
                (tec:TecnologiaComunicacao {num_estacao: rel.num_estacao, tecnologia: rel.tecnologia})
                WHERE rel.tipo_tecnologia IS NULL OR tec.tipo_tecnologia = rel.tipo_tecnologia
                CREATE (tec)-[op:TAXA_DOWNLOAD ]->(ant)
                with op,rel
                where rel.properties.frequencia_tx  is not null 
                SET op += rel.properties    
            """   
        add=[]
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        i=0
        relationships=''
        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 
            #filtrando_tecnologias= estacao.drop_duplicates(subset=['FreqTxMHz', 'FreqRxMHz'])[['FreqTxMHz', 'FreqRxMHz']].values
            tx_rx=''
            #ADICIONA TECNOLOGIA
            for id,info in estacao.iterrows():
                tipoTecnologia = '' if pd.isna(info['tipoTecnologia']) else f'tipo_tecnologia: \'{info['tipoTecnologia']}\','
                
                
                # if(not pd.isna(info['FreqTxMHz'])):
                #     tx_rx += f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\' }})  CREATE (ant{i})-[:TAXA_UPLOAD {{ frequencia_tx: {info['FreqTxMHz']} }} ]->(tec{i});"""
                #     # add.append([tx_rx])
                #     # self.conn.executeDB(tx_rx,database=mapabr)
                #     relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']}, {tipoTecnologia} properties: {{ frequencia_tx: {info['FreqTxMHz']} }} }},"""
                if(not pd.isna(info['FreqRxMHz'])):
                    tx_rx += f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}), (tec{i}:TecnologiaComunicacao {{ num_estacao: {estacao_erb}, tecnologia: \'{info['Tecnologia']}\'  }}) CREATE (tec{i})-[:TAXA_DOWNLOAD {{ frequencia_rx: {info['FreqRxMHz']} }}]->(ant{i});"""
                    # add.append([tx_rx])
                    # self.conn.executeDB(tx_rx,database=mapabr)
                    relationships+= f"""{{ uid: \'{info['_id']}\', num_estacao: {info['NumEstacao']}, tecnologia: \'{info['Tecnologia']}\', {tipoTecnologia} properties: {{ frequencia_rx: {info['FreqRxMHz']} }} }},"""

                i+=1
            # print(f'with [ {relationships[:-1]} ] as relationships  {query}')
            # print(tx_rx)
            self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}',database=mapabr)

           
            relationships=''
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]DownloadUpload/database_download_upload_{uf}.cypher',sep='\t',index=False,header=False)
  
    def adicionando_infraestrutura(self, uf, mapabr):
        clausulas = ''
        add_lote_inicial = '''UNWIND [''' 
        add_lote_final = '''] AS dado CREATE (n:InfraestruturaFisica { num_estacao: dado.num_estacao,
                             classificacao: dado.classificacao, compartilhamento:dado.compartilhamento} );'''

        add=[]
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        i=0
        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 
            filtrando_tecnologias=estacao.drop_duplicates(subset=['ClassInfraFisica',
            'CompartilhamentoInfraFisica'])[['ClassInfraFisica',
            'CompartilhamentoInfraFisica']].values
            infra = ''
            for info in filtrando_tecnologias:

                if(not pd.isna(info[0])):
                    if(pd.isna(info[1])):
                        clausulas+= f'{{ num_estacao: {estacao_erb}, classificacao: \'{info[0]}\' }},'

                    else:
                        clausulas += f'{{ num_estacao: {estacao_erb}, classificacao: \'{info[0]}\', compartilhamento:\'{info[1]}\' }} ,'
                    # add.append([infra])
            if(clausulas != ''):
                # print(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
                self.conn.executeDB(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}',database=mapabr)
                clausulas=''

        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]Infraestrutura/database_infraestrutura_{uf}.cypher',sep='\t',index=False,header=False)

    def conectando_antena_infraestrutura(self, uf,mapabr):

        query = """
               UNWIND relationships AS rel
                MATCH (ant:Antena { UID: rel.UID }),   
                (infra:InfraestruturaFisica  { num_estacao: rel.num_estacao, classificacao: rel.classificacao , compartilhamento:rel.compartilhamento }) 
                WHERE rel.compartilhamento IS NULL OR infra.compartilhamento = rel.compartilhamento
                CREATE (ant)-[:POSSUI_INFRA]->(infra); 
               """
        add=[]
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        i=0
        relationships = ''
        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 
            for id,info in estacao.iterrows():
                if not pd.isna(info['ClassInfraFisica']):
                    compartilhamento = f', compartilhamento:\'{info['CompartilhamentoInfraFisica']}\'' if not pd.isna(info['CompartilhamentoInfraFisica']) else ''
                    infra = f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}),   (infra{i}:InfraestruturaFisica  {{ num_estacao: {estacao_erb}, classificacao: \'{info['ClassInfraFisica']}\' {compartilhamento} }}) CREATE (ant{i})-[:POSSUI_INFRA]->(infra{i}); """
                    # add.append(infra)
                    # self.conn.executeDB(infra, database=mapabr)

                    

                    relationships += f'{{ UID: \'{info['_id']}\' ,  num_estacao: {estacao_erb}, classificacao: \'{info['ClassInfraFisica']}\' {compartilhamento} }},'
                i+=1
            if(relationships != ''):
                # print(f'with [ {relationships[:-1]} ] as relationships  {query}')
                self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}', database=mapabr)
                relationships = ''
        self.conn.executeDB(f'MATCH (infra: InfraestruturaFisica) REMOVE infra.num_estacao', database=mapabr)
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]AntenaInfra/database_antena_infra_{uf}.cypher',sep='\t',index=False,header=False)
    
    def adicionando_equipamento_modularizacao(self, uf,mapabr):
          
        clausulas = ''
        add_lote_inicial = '''UNWIND [''' 
        add_lote_final = '''] AS dado CREATE (n:Equipamento { num_estacao: dado.num_estacao, 
                            codigo_equipamento: dado.codigo_equipamento  , potencia_transmissao:  dado.potencia_transmissao });'''
        add=[]
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()

        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 

            filtrando_tecnologias= estacao.drop_duplicates(subset=[ 'CodEquipamentoTransmissor', 'PotenciaTransmissorWatts'])[[ 'CodEquipamentoTransmissor', 'PotenciaTransmissorWatts']].values

            for info in filtrando_tecnologias:
                autorizacao = f'num_estacao: {estacao_erb}'

                if(not pd.isna(info[0])):
                    ponto0 = str(info[0]).split(".")[0]
                    tira_char_especial = re.sub(r"[^a-zA-Z0-9\s]", '',ponto0)
                    # print( f'{type(info[0])} + " " + {info[0]} + " " + {ponto0} +" "+ {tira_char_especial} ' )
                    # print(f' "{tira_char_especial}" ')
                    autorizacao += f",codigo_equipamento: \'{tira_char_especial}\'  "
                if(not pd.isna(info[1])):
                    
                    decimal = MosaicoDB.converter_decimal(info[1],250)
                    autorizacao += f', potencia_transmissao:  {decimal}'
                clausulas+=f'{{ { autorizacao } }},'

            if clausulas != '':
                # print(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}')
                self.conn.executeDB(f'{add_lote_inicial} {clausulas[:-1]} {add_lote_final}', database=mapabr)
                clausulas=''

                
    
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]EquipamentoModularizacao/database_equipamento_modularizacao_{uf}.cypher',sep='\t',index=False,header=False)
                
    def conectando_antena_equipamento(self, uf, mapabr):
        # add=[]
        #  UNWIND relationships AS rel
        #        MATCH (ant:Antena { UID: rel.uid }),
        #         (tec:TecnologiaComunicacao {num_estacao: rel.num_estacao, tecnologia: rel.tecnologia})
        #         WHERE rel.tipo_tecnologia IS NULL OR tec.tipo_tecnologia = rel.tipo_tecnologia
        #         CREATE (tec)-[op:TAXA_DOWNLOAD ]->(ant)
        #         with op,rel
        #         where rel.properties.frequencia_tx  is not null 
        #         SET op += rel.properties    
        query = """
                UNWIND relationships AS rel
                MATCH (ant:Antena {UID: rel.UID }),  
                (equip:Equipamento { num_estacao: rel.num_estacao, codigo_equipamento: rel.codigo_equipamento, potencia_transmissao:rel.potencia_transmissao })  
                CREATE (ant)-[desigem:DESIGNACAO_EMISSOR ]->(equip)
                with desigem,rel
                WHERE rel.properties.designacao_emissao IS NOT NULL 
                set desigem += rel.properties
               """
        num_estacao = self.estacoes_smp['NumEstacao'].unique().tolist()
        i=0
        relationships=''
        for estacao_erb in num_estacao:
            estacao = self.estacoes_smp[self.estacoes_smp['NumEstacao']==estacao_erb] 
            
            for id,info in estacao.iterrows():
                if info['DesignacaoEmissao']=='':
                    relacionamento = f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}),  (equip{i}:Equipamento {{ num_estacao: {estacao_erb}, codigo_equipamento: \'{info['CodEquipamentoTransmissor']}\', potencia_transmissao:  {MosaicoDB.converter_decimal(info[ 'PotenciaTransmissorWatts'],250)} }})  CREATE (ant{i})-[:DESIGNACAO_EMISSOR]->(equip{i});"""
                    relationships+=f"""{{UID: \'{info['_id']}\' , num_estacao: {estacao_erb}, codigo_equipamento: \'{info['CodEquipamentoTransmissor']}\', potencia_transmissao:  {MosaicoDB.converter_decimal(info[ 'PotenciaTransmissorWatts'],250)} }},"""
                else:
                    desigem = f' {{designacao_emissao: \'{info['DesignacaoEmissao']}\' }} '
                    relacionamento = f"""MATCH (ant{i}:Antena {{UID: \'{info['_id']}\' }}),  (equip{i}:Equipamento {{ num_estacao: {estacao_erb}, codigo_equipamento: {info['CodEquipamentoTransmissor']}, potencia_transmissao:  {MosaicoDB.converter_decimal(info[ 'PotenciaTransmissorWatts'],250)} }})  CREATE (ant{i})-[:DESIGNACAO_EMISSOR {{designacao_emissao: \'{info['DesignacaoEmissao']}\' }}]->(equip{i});"""

                    relationships+=f"""{{UID: \'{info['_id']}\' , num_estacao: {estacao_erb}, codigo_equipamento: \'{info['CodEquipamentoTransmissor']}\', potencia_transmissao:  {MosaicoDB.converter_decimal(info[ 'PotenciaTransmissorWatts'],250)}, properties: {{designacao_emissao: \'{info['DesignacaoEmissao']}\' }} }},"""
                    i+=1
                # add.append([relacionamento])
            # print(f'with [ {relationships[:-1]} ] as relationships  {query}')
            self.conn.executeDB(f'with [ {relationships[:-1]} ] as relationships  {query}', database=mapabr)
            
            relationships=''
        self.conn.executeDB(f'MATCH (est:Equipamento) REMOVE est.num_estacao', database=mapabr)
        # df = pd.DataFrame(add, columns=['Nome'])
        # df.to_csv(f'[DATABASE]AntenaEquipamento/database_antena_equipamento_{uf}.cypher',sep='\t',index=False,header=False)


  




            



    def montando_estrutura(self, uf, mapabr):
        
        self.adicionando_antenas(uf, mapabr)
        print('adicionando_antenas')
        self.adicionando_estacoes(uf, mapabr)
        print('sistema.adicionar_estacoes')
        self.conectando_erb_antena(uf, mapabr)
        print('sistema.conectando_erb_antena')
        self.adicionando_tecnologias(uf, mapabr)
        print('sistema.adicionando_tecnologias')
        self.conectando_antena_tecnologia(uf, mapabr)
        print('sistema.conectando_antena_tecnologia')
        self.conectado_upload(uf, mapabr)
        print('sistema.conectado_upload')
        self.conectado_download(uf, mapabr)
        print('sistema.conectado_download')
        self.adicionando_infraestrutura(uf, mapabr)
        print('sistema.adicionando_infraestrutura')
        self.conectando_antena_infraestrutura(uf, mapabr)
        print('sistema.conectando_antena_infraestrutura')
        self.adicionando_equipamento_modularizacao(uf, mapabr)
        print('sistema.adicionando_equipamento_modularizacao')
        self.conectando_antena_equipamento(uf, mapabr)
        print('sistema.conectando_antena_equipamento')

teste = MosaicoDB()
teste.montando_estrutura('AC', 'testeacre')

