# auth_helpers.py
import os
import pymysql


# Access token 검증 함수
def verify_access_token(access_token, connection):
    try:
        cursor = connection.cursor()

        table_name = os.getenv('USER_ACCESS_TOKEN')

        sql_query = f"SELECT user_id FROM {table_name} WHERE token = %s"
        cursor.execute(sql_query, (access_token,))
        result = cursor.fetchone()

        cursor.close()
        connection.close()

        if result:
            return True, result[0]
        else:
            return False, None

    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False, None
