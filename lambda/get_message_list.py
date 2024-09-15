import json
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Conversation')

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError
    
def get_entry_count_by_thread_id(thread_id):
    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('thread_id').eq(thread_id),
            Select='COUNT'
        )
        count = response['Count']
        return count

    except Exception as e:
        print(f"Error: {e}")
        return None

def lambda_handler(event, context):
    try:
        thread_id = event['pathParameters']['thread_id']
        body = json.loads(event['body']) if event['body'] else {}
        
        page_size = int(body.get('pageSize', 10))
        
        page_number = int(body.get('pageNumber', 1))
        
        if not thread_id:
            raise ValueError("'thread_id' is required")

        query_params = {
            'KeyConditionExpression': boto3.dynamodb.conditions.Key('thread_id').eq(thread_id),
            'Limit': page_size * page_number,
            'ScanIndexForward': False
        }

        response = table.query(**query_params)
        
        messages = response.get('Items', [])

        start_index = (page_number - 1) * page_size
        end_index = start_index + page_size
        paged_messages = messages[start_index:end_index]

        message_list = [
            {
                'message_id': msg['message_id'],
                'role': msg['role'],
                'content': msg['content'],
                'created_at': msg['created_at']
            }
            for msg in paged_messages
        ]

        total_pages = (get_entry_count_by_thread_id(thread_id) + page_size - 1) // page_size

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message_list': message_list,
                'total_pages': total_pages,
                'current_page': page_number
            }, default=decimal_default),
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
