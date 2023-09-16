import json
import pandas as pd
import ast
import boto3
import pymysql

def lambda_handler(event, context):
    
    bucket_name = 'integrationflowbucket'
    object_key = '/tmp/movies_metadatas.csv'
    s3 = boto3.client('s3')
    
    rds_host = 'database-1.czoi0d5bgvz4.sa-east-1.rds.amazonaws.com'
    username = 'admin'
    password = 'New*3382'
    database_name = 'moviesdb'
    
    try:
        # Le bucket s3
        response = s3.get_object(Bucket=bucket_name,Key=object_key)
        df = pd.read_csv(response['Body'], delimiter=';')
        
        # Pega somente colunas desejadas
        df = df[['genres', 'id']]
        
        # Dropa nulos
        df = df.dropna(subset=['id', 'genres'])
        
        # Dropa campos vazios
        df = df.loc[df['id'] != '[]']
        df = df.loc[df['genres'] != '[]']
        
        # Renomea coluna id
        df = df.rename(columns={'id':'id_movies'})
        
        # Limpa coluna id
        df = df[df['id_movies'].str.contains(r'^[0-9]+$', regex=True)]
        df['id_movies'] = df['id_movies'].astype(int)
        
        # Remove duplicados de id
        df = df.drop_duplicates(subset='id_movies')
        
        # Define a funcao que extrai id e nome do dicionario
        def extrai_nome_id(s):
            try:
                lista_dados = ast.literal_eval(s)
                if lista_dados:  # Verificar se a lista nao esta vazia
                    ids = [entry['id'] for entry in lista_dados]
                    names = [entry['name'] for entry in lista_dados]
                else:
                    ids = []
                    names = []
                return ids, names
            except:
                return [], []
                
        # Cria um df temporario com a coluna genres divido em 2
        temp_df = df['genres'].apply(extrai_nome_id).apply(pd.Series)
        temp_df.columns = ['id_genres', 'name_genres']
        
        # Combina o df temp com o df orignal
        df = pd.concat([temp_df, df['id_movies']], axis=1)
        
        # Crie um DataFrame intermediário para cada coluna que deseja explodir
        df_name_genres = df[['name_genres', 'id_movies']].explode('name_genres')
        df_id_genres = df[['id_genres', 'id_movies']].explode('id_genres')
        
        # Resetar o índice dos DataFrames intermediários
        df_id_genres.reset_index(drop=True, inplace=True)
        df_name_genres.reset_index(drop=True, inplace=True)
        
        df_id_genres = df_id_genres.drop('id_movies', axis=1)
        
        # Concatenar os DataFrames intermediários
        df_new = pd.concat([df_id_genres, df_name_genres], axis=1)
        
        # Conexao RDS
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=database_name, connect_timeout=5)
        cursor = conn.cursor()
        
        # Criacao da tabela no RDS MySQL com a estrutura desejada
        create_table_query = """
        CREATE TABLE tbl_genres (
            id_genres INT NOT NULL,
            name_genres VARCHAR(255),
            id_movies INT NOT NULL,
            PRIMARY KEY (id_genres, id_movies)
        );
        """
        cursor.execute(create_table_query)
        
        # Carga dos dados transformados do DataFrame para a tabela
        for index, row in df_new.iterrows():
            sql = "INSERT INTO tbl_genres (id_genres, name_genres, id_movies) VALUES (%s, %s, %s)"
            values = int(row['id_genres']), row['name_genres'], int(row['id_movies'])
            cursor.execute(sql, values)
            
        conn.commit()
        conn.close()

        return{
            'statusCode': 200,
            'body': 'Arquivo CSV extraido, transformado e carregado com sucesso'
        }
    except Exception as e:
        return{
            'statusCode': 500,
            'body': f'Erro: {str(e)}'
        }
