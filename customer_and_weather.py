import pandas as pd
import json
import boto3
from botocore.exceptions import ClientError
import base64
import psycopg2
import requests
from datetime import datetime, timedelta
import pytz
import logging


def uploadtos3(file, access_key, secret_key, region_name, bkt=None, prefix=None):
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


def get_weather_data(api_key, lat, lon):
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}'
    try:
        r = requests.get(url)
        response = json.loads(r.text)  # Parse JSON response
        r.raise_for_status()  # Raise an exception for any HTTP errors
        df = pd.json_normalize(response)
        df['weather_id'] = df['weather'].apply(lambda x: x[0]['id'] if x else None)
        df['weather_type'] = df['weather'].apply(lambda x: x[0]['main'] if x else None)
        df['description'] = df['weather'].apply(lambda x: x[0]['description'] if x else None)
        df['date'] = df[['dt', 'timezone']].apply(
            lambda x: (datetime.utcfromtimestamp(int(x[0])) + timedelta(seconds=int(x[1]))).
            strftime('%Y-%m-%d %H:%M:%S'), axis=1)  # Format time
        df.rename(columns={'main.temp': 'temp', 'main.feels_like': 'feel_like', 'main.temp_min': 'min_temp',
                           'main.temp_max': 'max_temp', 'main.pressure': 'pressure', 'main.humidity': 'humidity',
                           'wind.speed': 'wind_speed', 'wind.deg': 'wind_deg', 'wind.gust': 'wind_gust',
                           'clouds.all': 'cloud_all',
                           'coord.lat': 'lat', 'coord.lon': 'lon'
                           }, inplace=True)
        cols = ['weather_id', 'weather_type', 'description', 'base', 'temp',
                'feel_like', 'min_temp', 'max_temp', 'pressure', 'humidity', 'visibility',
                'wind_speed', 'wind_deg', 'wind_gust', 'cloud_all', 'date', 'lat', 'lon']
        df = df[cols]
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def get_user_data(url):
    try:
        r = requests.get(url)
        response = json.loads(r.text)  # Parse json
        r.raise_for_status()  # Raise an exception for any HTTP errors
        df = pd.json_normalize(response)  #
        df.rename(columns={'address.suite': 'suite', 'address.city': 'city',
                           'address.zipcode': 'zipcode', 'address.geo.lat': 'lat',
                           'address.geo.lng': 'lon'
                           }, inplace=True)
        cols = ['id', 'name', 'username', 'email', 'phone', 'suite', 'city', 'zipcode', 'lat', 'lon']
        df = df[cols]
        return df
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None


def log_write(logfl):
    logformat = '[%(asctime)s] [%(filename)s] %(levelname)s %(message)s'
    logging.basicConfig(level=logging.INFO, filename=logfl, format=logformat)


def main():
    start_ts = str(datetime.now(pytz.timezone('Asia/Riyadh')))
    f = open('cred.json')  # Get credentials
    data = json.load(f)
    log_dir = 'log/customer'
    api_key = data['api_key']
    secret_key = data['secret_key']
    access_key = data['access_key']
    secret_redshift_name = data['secret_redshift_name']
    region_name = data['region_name']
    bucket_name = data['bucket_name']
    prefix = data['prefix']
    redshift_iam_role = data['redshift_iam_role']
    user_file = 'data/user_data.csv'
    weather_file = 'data/weather_data.csv'
    staging_schema = 'aiq_data_staging'
    schema_name = 'aiq_data'
    user_table_name = 'customer_details'
    weather_table_name = 'weather_details'
    logfl = f"{log_dir}/customer_and_weather_details.log.{start_ts}"
    try:
        log_write(logfl)  # Create log file
        #####################################
        ############# Get Users #############
        #####################################
        url = 'https://jsonplaceholder.typicode.com/users'
        df_user = get_user_data(url)
        rows, cols = df_user.shape
        print(df_user.head())
        logging.info(f"User Data Sample--> {df_user.head()}")
        df_user.to_csv(user_file, index=False)
        if rows > 0:
            #####################################
            ########## Get Weather Data ##########
            ######################################
            weather_list = []
            for lat, lon in df_user[['lat', 'lon']].drop_duplicates().values:
                w_df = get_weather_data(api_key, lat, lon)
                weather_list.append(w_df)
            df_weather = pd.concat(weather_list, ignore_index=True)
            print(df_weather.head())
            logging.info(f"Weather Data Sample--> {df_weather.head()}")
            df_weather.to_csv(weather_file, index=False)
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
            s3 = boto3.client('s3',
                              aws_access_key_id=access_key,
                              aws_secret_access_key=secret_key,
                              region_name=region_name,
                              )
            uploadtos3(user_file, access_key, secret_key, region_name, bucket_name, prefix)  # user file upload
            uploadtos3(weather_file, access_key, secret_key, region_name, bucket_name, prefix)  # weather file upload
            ##########################################
            ############ Upload to Redshift ##########
            ##########################################
            conn = get_conn(db, host, port, redshift_user, redshift_password)
            merge_qry_user = f"""begin;
                    truncate table {staging_schema}.{user_table_name};
                    copy {staging_schema}.{user_table_name} from 's3://{bucket_name}/{prefix}{user_file}' iam_role '{redshift_iam_role}' CSV QUOTE '\"' DELIMITER ',' ignoreheader 1 acceptinvchars;
                    delete from {schema_name}.{user_table_name} using {staging_schema}.{user_table_name} where 
                    {schema_name}.{user_table_name}.id = {staging_schema}.{user_table_name}.id;
                    insert into {schema_name}.{user_table_name} select * from {staging_schema}.{user_table_name};
                    end;"""
            merge_qry_weather = f"""begin;
                        truncate table {staging_schema}.{weather_table_name};
                        copy {staging_schema}.{weather_table_name} from 's3://{bucket_name}/{prefix}{weather_file}' iam_role '{redshift_iam_role}' CSV QUOTE '\"' DELIMITER ',' ignoreheader 1 acceptinvchars;
                        delete from {schema_name}.{weather_table_name} using {staging_schema}.{weather_table_name} where 
                        {schema_name}.{weather_table_name}.date = {staging_schema}.{weather_table_name}.date and 
                        {schema_name}.{weather_table_name}.lat = {staging_schema}.{weather_table_name}.lat and 
                        {schema_name}.{weather_table_name}.lon = {staging_schema}.{weather_table_name}.lon;
                        insert into {schema_name}.{weather_table_name} select * from {staging_schema}.{weather_table_name};
                        end;"""

            cur = conn.cursor()
            cur.execute(merge_qry_user)
            cur.execute(merge_qry_weather)
            conn.commit()
            conn.close()
            end_ts = str(datetime.now(pytz.timezone('Asia/Riyadh')))
            message = f"Started: {start_ts}, Rows: {rows} , Completed: {end_ts}"
            print(message)
            logging.info(message)
        else:
            print('No Data To Insert')
            logging.info("No Data To Insert")

    except Exception as error:
        exception_name = type(error).__name__
        print(f"failed with {error} and {exception_name}")
        logging.critical(f"{error} Error: {exception_name} Some Other Issue ")


if __name__ == '__main__':
    main()
