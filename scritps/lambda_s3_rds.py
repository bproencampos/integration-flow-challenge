import boto3
import pymysql
import pandas as pd

def lambda_handler(event, context):
    bucket_name = 'integrationflowbucket'
    object_key = '/tmp/movies_metadatas.csv'
    s3 = boto3.client('s3')
    
    rds_host = 'database-1.czoi0d5bgvz4.sa-east-1.rds.amazonaws.com'
    username = 'admin'
    password = 'New*3382'
    database_name = 'dbteste'

    try:
        # Le bucket s3
        response = s3.get_object(Bucket=bucket_name,Key=object_key)
        df = pd.read_csv(response['Body'], delimiter=';')
         
        # Efetua transformação do dado
        # Remove unnamed collumns
        for i in range(24, 27):
            df = df.drop(columns=[f'Unnamed: {i}'])

        # Seleciona colunas desejadas
        df = df[['id', 'title', 'revenue', 'budget', 'release_date', 'vote_count']]

        # Deleta nulos
        df = df.dropna(subset=['id', 'title', 'revenue', 'budget', 'release_date', 'vote_count'])

        # Limpa coluna id
        df = df[df['id'].str.contains(r'^[0-9]+$', regex=True)]
        df['id'] = df['id'].astype(int)

        # Limpa coluna revenue
        df = df[~df['revenue'].str.contains(r'\d{2}/\d{2}/\d{4}', regex=True)]
        df['revenue'] = df['revenue'].astype(float)

        # Budget to float
        df['budget'] = df['budget'].astype(float)

        # Limpa release_date
        df = df[df['release_date'].str.contains(r'\d{2}/\d{2}/\d{4}', regex=True)]
        df['release_date'] = df['release_date'].astype(str)

        # vote_count to int
        df['vote_count'] = df['vote_count'].astype(int)

        # Cria coluna lucro
        df['lucro'] = df['revenue'] - df['budget']

        # Remove valores onde lucro = 0
        df = df[df['revenue'] != 0]

        df['lucro'] = df['lucro'].astype(float)

        def formatar_monetario(valor):
            return '{:.2}'.format(valor)

        df['lucro'] = df['lucro'].apply(formatar_monetario)

        df_without_header = df.copy()
        df_without_header.columns = range(df_without_header.shape[1])
        
        # Conexao RDS
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=database_name, connect_timeout=5)
        cursor = conn.cursor()
        
        # Criacao da tabela no RDS MySQL com a estrutura desejada
        create_table_query = """
        CREATE TABLE tbl_movies (
            id INT NOT NULL,
            title VARCHAR(255),
            revenue FLOAT,
            budget FLOAT,
            release_date VARCHAR(15),
            vote_count int,
            lucro FLOAT,
            PRIMARY KEY (id)
        );
        """
        cursor.execute(create_table_query)
        
        # Carga dos dados transformados do DataFrame para a tabela
        for index, row in df_without_header.iterrows():
            sql = "INSERT INTO tbl_movies (id, title, revenue, budget, release_date, vote_count, lucro) VALUES (%i, %s, %f, %f, %s, %d, %f)"
            values = (row['0'], row['1'], row['2'], row['3'], row['4'], row['5'], row['6'])
            cursor.execute(sql, values)
            
        conn.commit()
        conn.close()

        return{
            'statusCode': 200,
            'body': 'Arquivo CSV lido com sucesso'
        }
    except Exception as e:
        return{
            'statusCode': 500,
            'body': f'Erro: {str(e)}'
        }