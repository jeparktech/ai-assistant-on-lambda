import json
import boto3
from openai import OpenAI

secrets_client = boto3.client('secretsmanager')

dynamodb = boto3.resource('dynamodb')
thread_table = dynamodb.Table('Thread')
convo_table = dynamodb.Table('Conversation')
user_table = dynamodb.Table('User')


def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret['OPENAI_API_KEY']
    except Exception as e:
        raise Exception(f"Unable to retrieve secret: {str(e)}")
    
def get_user_from_dynamodb(user_id):
    """DynamoDB에서 thread_id에 해당하는 user_id를 가져옵니다."""
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
    
def get_assistant_id_from_dynamodb(thread_id):
    """DynamoDB에서 thread_id에 해당하는 assistant_id를 가져옵니다."""
    try:
        response = thread_table.get_item(
            Key={'thread_id': thread_id}
        )
        item = response.get('Item')
        if not item:
            raise ValueError(f"Thread ID {thread_id} not found in DynamoDB")
        
        assistant_id = item.get('assistant_id')
        if not assistant_id:
            raise ValueError(f"No assistant_id found for thread_id {thread_id}")

        return assistant_id

    except Exception as e:
        raise Exception(f"Error retrieving assistant_id from DynamoDB: {str(e)}")
    
def save_message_to_dynamodb_from_openai_message(message):
    """OpenAI의 Message 객체를 DynamoDB에 저장"""
    try:
        message_id = message.id
        assistant_id = message.assistant_id
        role = message.role
        thread_id = message.thread_id
        created_at = message.created_at

        content = "\n".join([
            block.text.value
            for block in message.content
            if hasattr(block, 'text') and hasattr(block.text, 'value')
        ])

        convo_table.put_item(
            Item={
                'thread_id': thread_id,
                'message_id': message_id,
                'role': role,
                'content': content,
                'created_at': created_at,
                'assistant_id': assistant_id
            }
        )

    except Exception as e:
        raise Exception(f"Error saving message to DynamoDB: {str(e)}")

def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        user_id = body['userId']
        message_content = body['message']
        thread_id = event['pathParameters']['thread_id']
        # body = json.loads(event['body'])
        # user_id = body.get('user_id')
        # message_content = body.get('message')

        user_id = get_user_from_dynamodb(user_id)
        assistant_id = get_assistant_id_from_dynamodb(thread_id)

        if not user_id or not message_content:
            raise ValueError("Both 'user_id' and 'message' are required")
        
        if not thread_id:
            raise ValueError("'thread_id' is required")
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }

    try:
        client = OpenAI(api_key=get_secret('prod/earthmera'))

        message = client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=message_content
        )
        save_message_to_dynamodb_from_openai_message(message)
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=assistant_id,
            instructions="Continue assisting the user based on the current thread context."
        )

        if run.status == 'completed':
            messages = client.beta.threads.messages.list(
                thread_id=thread_id
            )

            latest_message = messages.data[0] 
            save_message_to_dynamodb_from_openai_message(latest_message)
            latest_text = "\n".join([
                block.text.value
                for block in latest_message.content
                if hasattr(block, 'text') and hasattr(block.text, 'value')
            ])

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Message sent successfully',
                    'response': latest_text
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        else:
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Assistant is still processing',
                    'status': run.status
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