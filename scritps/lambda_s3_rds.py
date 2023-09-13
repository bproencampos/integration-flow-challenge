import os
import csv
import boto3
import pymysql
import pandas as pd

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    rds_host = 'database-1.czoi0d5bgvz4.sa-east-1.rds.amazonaws.com'
    username = 'admin'
    password = 'New*3382'
    database_name = 'database-1'

    try:
        # Informacoes do objeto s3
        bucket_name = event['Records'][0]['s3']['bucket']['integrationflowbucket']
        object_key = event['Records'][0]['object']['/tmp/movies_metadatas.csv']

        # Conexao RDS
        conn = pymysql.connect(host=rds_host, user=username, password=password, database=database_name)
        cursor = conn.cursor()

        # Le arquivo do S3 e cria tabela no RDS com os dados
        s3_client.download_file(bucket_name, object_key, '/tmp/movies_metadatas.csv')
        df = pd.read_csv('/tmp/movies_metadatas.csv')

        # Efetua transformação do dado
        # Remove unnamed collumns
        for i in range(24, 27):
            df = df.drop(columns=[f'Unnamed: {i}'])
        
        # Seleciona somente as colunas necessarias
        df = df[['id', 'title', 'genres', 'revenue', 'budget', 'release_date', 'vote_count']]

        # Remove valores nulos
        df = df.dropna(subset=['title', 'revenue', 'budget', 'release_date', 'vote_count'])

        # Transformar valores string em NaN para depois dropá-los
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

        # Remove NaN da coluna revenue
        df = df.dropna(subset='revenue')

        # Transformar valores string em NaN para depois dropá-los
        df['budget'] = pd.to_numeric(df['budget'], errors='coerce')

        # Remove NaN da coluna revenue
        df = df.dropna(subset='budget')

        # Cria coluna lucro
        df['lucro'] = df['revenue'] - df['budget']

        # Remove valores onde lucro = 0
        df = df[df['revenue'] != 0]

        # Funcao que formata a coluna lucro para monetario
        def formatar_monetario(valor):
            return '{:.2}'.format(valor)

        df['lucro'] = df['lucro'].apply(formatar_monetario)

        # Criacao da tabela no RDS MySQL com a estrutura desejada
        create_table_query = """
        CREATE TABLE nova_tabela (
            coluna1 VARCHAR(255),
            coluna2 INT,
            preco FLOAT
        );
        """
        cursor.execute(create_table_query)

        # Carga dos dados transformados do DataFrame para a tabela
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO nova_tabela (coluna1, coluna2, preco) VALUES (%s, %s, %s)",
                           (row['coluna1'], row['coluna2'], row['preco']))

        conn.commit()
        conn.close()

    except Exception as e:
        print(f'Erro: {e}')
        raise e

    return {
        'statusCode': 200,
        'body': 'Transferência para o RDS concluída com sucesso'
    }