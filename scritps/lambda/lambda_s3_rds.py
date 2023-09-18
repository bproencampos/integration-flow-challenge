import boto3
import json
import pymysql
import pandas as pd
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    bucket_name = 'integrationflowbucket'
    object_key = '/tmp/movies_metadatas.csv'
    s3 = boto3.client('s3')

    try:
        # Le bucket s3
        response = s3.get_object(Bucket=bucket_name,Key=object_key)
        df = pd.read_csv(response['Body'], delimiter=';')
         
        # Efetua transformação do dado
        # Remove unnamed collumns
        for i in range(24, 27):
            df = df.drop(columns=[f'Unnamed: {i}'])

        df = df.rename(columns={'id': 'id_movies'})

        # Seleciona colunas desejadas
        df = df[['id_movies', 'title', 'revenue', 'budget', 'release_date', 'vote_average', 'vote_count']]

        # Deleta nulos
        df = df.dropna(subset=['id_movies', 'title', 'revenue', 'budget', 'release_date',  'vote_average', 'vote_count'])

        # Limpa coluna id
        df = df[df['id_movies'].str.contains(r'^[0-9]+$', regex=True)]
        df['id_movies'] = df['id_movies'].astype(int)

        # Limpa coluna revenue
        df = df[~df['revenue'].str.contains(r'\d{2}/\d{2}/\d{4}', regex=True)]
        df['revenue'] = df['revenue'].astype(float)
        
        df['vote_average'] = df['vote_average'].astype(float)

        # Budget to float
        df['budget'] = df['budget'].astype(float)

        # Limpa release_date
        df = df[df['release_date'].str.contains(r'\d{2}/\d{2}/\d{4}', regex=True)]
        df['release_date'] = df['release_date'].astype(str)

        # vote_count to int
        df['vote_count'] = df['vote_count'].astype(int)

        # Cria coluna lucro
        df['lucro'] = df['revenue'] - df['budget']

        df['lucro'] = df['lucro'].astype(float)

        # Remove valores = 0 de lucro
        df = df.loc[df['lucro'] != 0.0]
        
        # Remove duplicados de id
        df = df.drop_duplicates(subset='id_movies')

        # Define funcao que formata campo lucro
        def formata_lucro(valor_lucro):
            valor_numerico = float(valor_lucro)
            valor_formatado = "${:,.2f}".format(valor_numerico)
            return valor_formatado

        # Aplica funcao de formatacao
        df['lucro'] = df['lucro'].apply(lambda x: formata_lucro(x))
        
        def get_secret():
    
            secret_name = "rds!db-e240d9d9-80dc-4156-8c85-e6e6ed08459f"
            region_name = "sa-east-1"
        
            # Create a Secrets Manager client
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=region_name
            )
        
            try:
                get_secret_value_response = client.get_secret_value(
                    SecretId=secret_name
                )
            except ClientError as e:
                # For a list of exceptions thrown, see
                # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                raise e
        
            # Decrypts secret using the associated KMS key.
            secret = get_secret_value_response['SecretString']
            
            return secret
        
        database_credentials = json.loads(get_secret())
        
        rds_host = 'database-1.czoi0d5bgvz4.sa-east-1.rds.amazonaws.com'
        username = 'admin'
        password = database_credentials['password']
        database_name = 'moviesdb'
        table_name = 'tbl_movies'
        
        # Conexao RDS
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=database_name, connect_timeout=20)
        cursor = conn.cursor()
        
        # Consulta SQL para verificar se a tabela existe
        query = f"SHOW TABLES LIKE '{table_name}'"
    
        cursor.execute(query)
    
        # Verificar se a tabela existe
        if cursor.fetchone():
            # Esvazia tabela
            create_table_query = """
            DELETE FROM tbl_movies;
            """
            cursor.execute(create_table_query)
        else:
            # Criacao da tabela no RDS MySQL com a estrutura desejada
            create_table_query = """
            CREATE TABLE tbl_movies (
                id_movies INT NOT NULL,
                title VARCHAR(255),
                revenue FLOAT,
                budget FLOAT,
                release_date VARCHAR(15),
                vote_average FLOAT,
                vote_count int,
                lucro VARCHAR(50),
                PRIMARY KEY (id_movies)
            );
            """
            cursor.execute(create_table_query)
        
        # Carga dos dados transformados do DataFrame para a tabela
        for index, row in df.iterrows():
            sql = "INSERT INTO tbl_movies (id_movies, title, revenue, budget, release_date, vote_average, vote_count, lucro) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            values = int(row['id_movies']), row['title'], float(row['revenue']), float(row['budget']), row['release_date'], float(row['vote_average']), int(row['vote_count']), row['lucro']
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
