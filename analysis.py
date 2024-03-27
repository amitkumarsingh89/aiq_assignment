import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
import base64
import psycopg2
import pytz
from datetime import datetime, timedelta
import logging

def get_conn(db, host, port, user, password):
    """
    This function will use credentials from secret manager to create connection with DB.
    """
    try:
        engine = psycopg2.connect(dbname=db, host=host,
                                  port=port, user=user,
                                  password=password)

        print('connection engine created')

        return engine
    except Exception as e:
        raise e

def main():
    start_ts = str(datetime.now(pytz.timezone('Asia/Riyadh')))
    interval = str(datetime.now(pytz.timezone('Asia/Riyadh'))-timedelta(days = 60)) # Timezone can be changed based on our requirement
    log_dir = 'log/sales'
    f = open('cred.json') # Get credentials
    data = json.load(f)
    secret_key = data['secret_key']
    access_key = data['access_key']
    secret_redshift_name = data['secret_redshift_name']
    region_name = data['region_name']
    bucket_name = data['bucket_name']
    prefix = data['prefix']
    redshift_iam_role = data['redshift_iam_role']
    staging_schema = 'aiq_data_staging'
    schema_name = 'aiq_data'
    table_name = 'sales_order'
    logfl = f"{log_dir}/{table_name}.log.{start_ts}"

    qry_agg = """select count(distinct order_id) as orders, sum(price) as rev, sum(quantity) as qty,
               round(sum(quantity)::numeric/count(distinct order_id),2) as aoq, 
               round(sum(price)::numeric/count(distinct order_id),2) as aov
               from aiq_data.sales_order
              """
    qry_orders_by_prodct = """select product_id, count(distinct order_id) as orders, sum(price) as rev, sum(quantity) as qty,
                                round(sum(quantity)::numeric/count(distinct order_id),2) as aoq,
                                round(sum(price)::numeric/count(distinct order_id),2) as aov
                                from aiq_data.sales_order group by 1 order by 3 desc"""








