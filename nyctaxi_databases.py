import os
import time
import boto3

client = boto3.client('athena')


print('Cria database "nyctaxi" em Athena')
query = 'CREATE DATABASE nyctaxi'
response = client.start_query_execution(
    QueryString = query,
    ResultConfiguration = {'OutputLocation': 's3://ytbd-datasprints/athena/'}
)

print('Cria tabela "nyctaxi"."payment" em Athena com base em "s3://ytbd-datasprints/payment"')
query = 'CREATE EXTERNAL TABLE IF NOT EXISTS nyctaxi.payment (' \
        'payment_type string,' \
        'payment_lookup string' \
        ')' \
        'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\'' \
        'LOCATION \'s3://ytbd-datasprints/payment\'' \
        'TBLPROPERTIES (' \
        '"skip.header.line.count"="2"' \
        ')'
response = client.start_query_execution(
    QueryString = query,
    ResultConfiguration = {'OutputLocation': 's3://ytbd-datasprints/athena/'}
)

print('Cria tabela "nyctaxi"."vendor" em Athena com base em "s3://ytbd-datasprints/vendor"')
query = 'CREATE EXTERNAL TABLE IF NOT EXISTS nyctaxi.vendor (' \
        'vendor_id string,' \
        'name string,' \
        'address string,' \
        'city string,' \
        'state string,' \
        'zip int,' \
        'country string,' \
        'contact string,' \
        'current_contact string' \
        ')' \
        'ROW FORMAT SERDE \'org.apache.hadoop.hive.serde2.OpenCSVSerde\'' \
        'LOCATION \'s3://ytbd-datasprints/vendor\'' \
        'TBLPROPERTIES (' \
        '"skip.header.line.count"="1"' \
        ')'
response = client.start_query_execution(
    QueryString = query,
    ResultConfiguration = {'OutputLocation': 's3://ytbd-datasprints/athena/'}
)

print('Cria tabela "nyctaxi"."trips" em Athena com base em "s3://ytbd-datasprints/trips"')
query = 'CREATE EXTERNAL TABLE IF NOT EXISTS nyctaxi.trips (' \
        'vendor_id string,' \
        'pickup_datetime string,' \
        'dropoff_datetime string,' \
        'passenger_count int,' \
        'trip_distance decimal(10,2),' \
        'pickup_longitude decimal(10,6),' \
        'pickup_latitude decimal(10,6),' \
        'rate_code string,' \
        'store_and_fwd_flag string,' \
        'dropoff_longitude decimal(10,6),' \
        'dropoff_latitude decimal(10,6),' \
        'payment_type string,' \
        'fare_amount decimal(10,2),' \
        'surcharge decimal(10,2),' \
        'tip_amount decimal(10,2),' \
        'tolls_amount decimal(10,2),' \
        'total_amount decimal(10,2)' \
        ')' \
        'ROW FORMAT SERDE \'org.apache.hive.hcatalog.data.JsonSerDe\'' \
        'LOCATION \'s3://ytbd-datasprints/trips\''
response = client.start_query_execution(
    QueryString = query,
    ResultConfiguration = {'OutputLocation': 's3://ytbd-datasprints/athena/'}
)
