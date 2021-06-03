import sys
import logging
import config
import pymysql
import json

# rds settings
rds_host = "asset-manager-db.c9zxukrtlwzh.us-east-2.rds.amazonaws.com"
name = config.db_username
password = config.db_password
db_name = config.db_name

print(rds_host)
# logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# connect using creds from rds_config.py
try:
    conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, port=3306)
except Exception as e:
    print(e)
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS mysql instance succeeded")


def check_duplicate(barcode_id):
    cursor = conn.cursor()
    sql = "SELECT * FROM `tbl_assets` WHERE barcode_id = (%s)"
    cursor.execute(sql, barcode_id)

    rows = cursor.fetchall()
    if len(rows) > 0:
        return False
    return True


# executes upon API event
def lambda_handler(event, context):
    request_body = event["body"]
    data = json.loads(request_body)
    print(data)
    if check_duplicate(data['barcode_id']) is False:
        return {
            'statusCode': 401,
            "body": json.dumps({
                "message": "barcode_id have been registered"
            })
        }
    with conn.cursor() as cur:
        sql = "INSERT INTO `tbl_assets` (`barcode_id`,`asset_type`, `description`) VALUES (%s, " \
              "%s, %s) "
        cur.execute(sql, (data['barcode_id'], data['asset_type'], data['description']))
        conn.commit()

    return {
        'statusCode': 200,
        "body": json.dumps({
            "message": "Create successfully"
        })
    }
