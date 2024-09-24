import json
import boto3
import os
import pymysql
from openai import OpenAI
from datetime import datetime
from auth_helper import verify_access_token

secrets_client = boto3.client('secretsmanager')

dynamodb = boto3.resource('dynamodb')
user_table = dynamodb.Table('User')

def get_secret(secret_name, secret_string):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret[secret_string]
    except Exception as e:
        raise Exception(f"Unable to retrieve secret: {str(e)}")
    
def get_user_from_dynamodb(user_id):
    """DynamoDB에서 thread_id에 해당하는 assistant_id를 가져옵니다."""
    try:
        response = user_table.get_item(
            Key={'user_id': user_id}
        )
        item = response.get('Item')
        if not item:
            raise ValueError(f"User ID {user_id} not found in DynamoDB")
        
        return user_id

    except Exception as e:
        raise Exception(f"Error retrieving user_id from DynamoDB: {str(e)}")
    
def connect_to_rds():
    try:
        connection = pymysql.connect(
            host=os.getenv('RDS_HOST'),
            user=get_secret(os.getenv('SECRET_MANAGER_NAME'), 'username'),
            password=get_secret(os.getenv('SECRET_MANAGER_NAME'), 'password'),
            database=os.getenv('DB_NAME'),
            connect_timeout=5
        )
        return connection
    except Exception as e:
        print(f"ERROR: Unable to connect to MySQL instance. {str(e)}")
        raise e
    

def lambda_handler(event, context):
    try:
        connection = connect_to_rds()

        auth_header = event['headers'].get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise ValueError('Missing or invalid Authorization header')
        access_token = auth_header.split(' ')[1]

        is_valid_token, token_user_id = verify_access_token(access_token, connection)
        if not is_valid_token:
            return {
                'statusCode': 401,
                'body': json.dumps({'error': 'Unauthorized - Invalid access token'})
            }

        get_user_from_dynamodb(token_user_id)

    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }


    try:
        client = OpenAI(api_key=get_secret('prod/earthmera', 'OPENAI_API_KEY'))
        assistant_id = "asst_iq0TlYEMvruN29nxKPtttiJt"
        thread = client.beta.threads.create()

        thread_id = thread.id
        created_at = datetime.utcnow().isoformat()

        # DynamoDB에 thread 저장
        table = dynamodb.Table('Thread')
        table.put_item(
            Item={
                'thread_id': thread_id,
                'assistant_id': assistant_id,
                'created_at': created_at,
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Thread created successfully',
                'thread_id': thread_id,
                'assistant_id': assistant_id,
                'created_at': created_at
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
