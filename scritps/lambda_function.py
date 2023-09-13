import boto3
import os
import requests # Para interagir com o Github

def lambda_handler(event, context):
    
    # Informacoes do arquivo no Github
    github_user = 'bproencampos'
    github_repo = 'integration-flow-challenge'
    github_file_path = 'bproencampos/integration-flow-challenge/movies_metadatas.csv'
    
    # URL do arquivo
    github_url = f'https://raw.githubusercontent.com/bproencampos/integration-flow-challenge/develop/data_source/movies_metadatas.csv'
    
    # Nome do arquivo no S3
    bucket_name = 'integrationflowbucket'
    filename = '/tmp/movies_metadatas.csv'    # Precisa ser TMP????
    
    # Cria cliente s3
    s3 = boto3.client('s3')
    
    try:
        # Executa download
        response = requests.get(github_url)
        if response.status_code == 200:
            # Carrega conteudo do arquivo no s3
            s3.put_object(Bucket=bucket_name,key=filename,Body=response.text)
            return{
                'statusCode': 200,
                'body': 'Arquivo CSV carregado com sucesso no S3.'
            }
        else:
            return {
                'statusCode': response.status_code,
                'body': 'Erro ao executar download do arquivo do Github'
            }
    except Exception as e:
        return{
            'statusCode': 500,
            'body': str(e)
        }