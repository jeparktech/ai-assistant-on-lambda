import json
import boto3
from openai import OpenAI
import openai
from datetime import datetime
import uuid

secrets_client = boto3.client('secretsmanager')

dynamodb = boto3.resource('dynamodb')
user_table = dynamodb.Table('User')



def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret['OPENAI_API_KEY']
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

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        user_id = body['userId']
        user_id = get_user_from_dynamodb(user_id)
        if not user_id:
            raise ValueError("user_id is required")
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }


    try:
        client = OpenAI(api_key=get_secret('prod/earthmera'))
        assistant_id = "YOUR_ASSISTANT_ID"
        thread = client.beta.threads.create()

        thread_id = thread.id
        created_at = datetime.utcnow().isoformat()

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Thread')
        table.put_item(
            Item={
                'thread_id': thread_id,
                'user_id': user_id,
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
