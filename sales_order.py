import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
import base64
import psycopg2
import pytz
from datetime import datetime, timedelta
import logging

def uploadtos3(file,access_key, secret_key, region_name,  bkt=None, prefix=None):
    """Function to upload files to S3
        Parameters:
            file(string)  : Name of the file that needs to be uploaded
            bkt(string)   : S3 bucket name
            prefix(string): S3 key where the file needs to be uploaded

    """
    global bucket, key
    if bkt is None:
        bkt = bucket
    if prefix is None:
        prefix = key
    try:
        s3 = boto3.client('s3',
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key,
                          region_name=region_name
                          )
        print("*****", file, bkt, prefix)
        s3.upload_file(file, bkt, prefix + file)
        print('file uploaded successfully!')
    except ClientError as e:
        raise e
def get_secret(secret_name, region_name, access_key, secret_key):
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret

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

def log_write(logfl):
    logformat = '[%(asctime)s] [%(filename)s] %(levelname)s %(message)s'
    logging.basicConfig(level=logging.INFO, filename=logfl, format=logformat)

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
    try:
        log_write(logfl)  # Create log file
        df_sales = pd.read_csv('data/sales_data.csv') # Read sales data (this data should be read for last 90 days to run upsert command
        #df_sales = df_sales[df_sales['order_date'] >= interval]
        print(df_sales.head())
        logging.info(f"Sales Data Sample --> {df_sales.head()}")
        new_rows, new_cols = df_sales.shape
        if new_rows > 0:
            ##### Redshift Credentials #########
            secret_redshift = json.loads(get_secret(secret_redshift_name, region_name, access_key, secret_key))
            redshift_password = secret_redshift['password']
            redshift_user = secret_redshift['username']
            db = secret_redshift['database']
            host = secret_redshift['host']
            port = secret_redshift['port']
            ##########################################
            ############## S3 Upload #################
            ##########################################
            csv_file = 'data/sales_data.csv'
            s3 = boto3.client('s3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
                )
            uploadtos3(csv_file, access_key, secret_key, region_name, bucket_name, prefix)
            ##########################################
            ############ Upload to Redshift ##########
            ##########################################
            conn = get_conn(db, host, port, redshift_user, redshift_password)
            merge_qry = f"""begin;
                    truncate table {staging_schema}.{table_name};
                    copy {staging_schema}.{table_name} from 's3://{bucket_name}/{prefix}{csv_file}' iam_role '{redshift_iam_role}' CSV QUOTE '\"' DELIMITER ',' ignoreheader 1 acceptinvchars;
                    delete from {schema_name}.{table_name} using {staging_schema}.{table_name} where 
                    {schema_name}.{table_name}.order_id = {staging_schema}.{table_name}.order_id;
                    insert into {schema_name}.{table_name} select * from {staging_schema}.{table_name};
                    end;"""
            print(merge_qry)
            cur = conn.cursor()
            cur.execute(merge_qry)
            conn.commit()
            conn.close()
            end_ts = str(datetime.now(pytz.timezone('Asia/Riyadh')))
            message = f"Started: {start_ts}, Rows: {new_rows} , Completed: {end_ts}"
            print(message)
            logging.info(message)
        else:
            print('No data to insert')
            logging.info('No data to insert')
    except Exception as error:
        exception_name = type(error).__name__
        print(f"failed with {error} and {exception_name}")
        logging.critical(f"{error} Error: {exception_name} Some Other Issue ")

if __name__ == '__main__':
    main()