# NYC TAXI TRIPS

Esse projeto realiza a análise de base de dados de corrida de taxi em Nova York no período de 2009 a 2012, considerando caracteŕisticas como tempo entre corridas, existência de gorjetas, valores em dinheiro, volume de corridas por mês ou dia.


## Arquitetura Geral

A arquitetura utiliza infraestrutura da AWS para armazenamento, processamento e disponibilização de data warehouse e data lake para consumo de dados.

Escolheu-se a AWS por ser referência no mercado de serviços em cloud com grande representatividade, completa com simplicidade de desenvolvimento, qualidade, custos baixos, alta escalabilidade.

![Arquitetura Geral](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/arquitetura_geral.png)

Os arquivos com dados são armazenados no serviço Amazon S3 no bucket **ytbd-nyctaxi**, sendo distribuído em diretórios:
- nyctaxi-raw: pasta para ingestão de dados originais sem transformação
- nyctaxi-curated: pasta para arquivos filtrados e com transformações simples quando aplicadas
- nyctaxi-history: pasta para cópia de arquivos originais para histórico, removidos da raw após transferência para curated
- nyctaxi-lakehouse: pasta estruturada e segmentada representando um data lakehouse para consumo de dados 

Foram desenvolvidos serviços Amazon Lambda para identificação de eventos de novos arquivos no bucket e processamento:
- nyctaxi-lambda-s3-raw: identifica inserção de novos arquivos na pasta raw, copia o arquivo para estrutura de curated e para history e por fim remove o arquivo da raw
- nyctaxi-lambda-s3-curated: identifica inserço de novos arquivos na pasta curated, transforma o arquivo para a pasta lakehouse (nesse momento apenas copia) e mantém o arquivo também na pasta curated

Neste momento o provisionamento da infraestrutura ainda não foi versionalizado e realizado automaticamente com ferramentas como o TerraForm (esta é uma próxima etapa deste projeto).


## Ingestão de dados

Os dados foram extraídos da base disponibilizada, compactados em formato bzip2/bz2 e armazenados no bucket **ytbd-nyctaxi** no diretório **nyctaxi-raw** (ao armazenar o dado compactado reduz-se a tranferência de dados e custo de consumo).

A compactação foi realizada via command no Linux (arquivos na pasta local "data"):

- bzip2 -c data/data-payment_lookup-csv.csv > data/payment.bz2
- bzip2 -c data/data-vendor_lookup-csv.csv > data/vendor.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2009-json_corrigido.json > data/trips2009.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2010-json_corrigido.json > data/trips2010.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2011-json_corrigido.json > data/trips2011.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2012-json_corrigido.json > data/trips2012.bz2

A criação e atualização no bucket foi realizada utilizando comandos AWS CLI (autenticado localmente):

- aws s3api create-bucket --bucket ytbd-nyctaxi
- aws s3 cp data/payment.bz2 s3://ytbd-nyctaxi/nyctaxi-raw/payment/payment.bz2
- aws s3 cp data/vendor.bz2 s3://ytbd-nyctaxi/nyctaxi-raw/vendor/vendor.bz2
- aws s3 cp data/trips2009.bz2 s3://ytbd-nyctaxi/nyctaxi-raw/trips/trips2009.bz2
- aws s3 cp data/trips2010.bz2 s3://ytbd-nyctaxi/nyctaxi-raw/trips/trips2010.bz2
- aws s3 cp data/trips2011.bz2 s3://ytbd-nyctaxi/nyctaxi-raw/trips/trips2011.bz2
- aws s3 cp data/trips2012.bz2 s3://ytbd-nyctaxi/nyctaxi-raw/trips/trips2012.bz2


## Função nyctaxi-lambda-s3-raw

A função identifica inserção de novos arquivos na pasta raw, copia o arquivo para estrutura de curated e para history (renomeia arquivo com data e hora no formato yyyymmdd-hhmmss-<<arquivo.extensão>>) e por fim remove o arquivo da pasta raw.

**Role nyctaxi-role-s3-raw**

Foi aplicada a role **nyctaxi-role-s3-raw** com as políticas:
- AmazonS3FullAccess

```
{
  "Version": "2012-10-17",
  "Id": "default",
  "Statement": [
    {
      "Sid": "lambda-bbabdfdb-c0b6-4a1c-bbcc-0a1fce603700",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:716124686595:function:nyctaxi-lambda-s3-raw",
      "Condition": {
        "StringEquals": {
          "AWS:SourceAccount": "716124686595"
        },
        "ArnLike": {
          "AWS:SourceArn": "arn:aws:s3:::ytbd-nyctaxi"
        }
      }
    }
  ]
}
```

**Código função Lambda**

```
import json
import urllib.parse
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Recupera objeto do evento
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # Move para curated e history (glacial) zone
    try:

        response = s3.get_object(Bucket=bucket, Key=key)
        obj_content_type = response['ContentType']

        key_curated = key.replace('nyctaxi-raw','nyctaxi-curated')
        key_folder = key[:key.rfind('/')].replace('nyctaxi-raw','nyctaxi-history')
        key_file = key[key.rfind('/')+1:]
        key_date = datetime.now().strftime("%Y%m%d-%H%M%S")
        key_history = '{}/{}-{}'.format(key_folder, key_date, key_file)

        # Novo arquivo identificado
        print('Novo arquivo identificado: {}/{}'.format(bucket, key))

        # Copia arquivo para área de curated zone
        print('Copia arquivo para área de curated zone: {}/{}'.format(bucket, key_curated))
        copy_source = { 'Bucket' : bucket, 'Key' : key }
        s3.copy(copy_source, bucket, key_curated)

        # Copia arquivo para área de histórico
        print('Copia arquivo para área de histórico: {}/{}'.format(bucket, key_history))
        copy_source = { 'Bucket' : bucket, 'Key' : key }
        s3.copy(copy_source, bucket, key_history)

        # Remove arquivo de raw zone
        s3d = boto3.resource('s3')
        s3d.Object(bucket, key).delete()

        return bucket, key, obj_content_type

    except Exception as e:

        print(e)
        print('Erro ao recuperar / traballhar arquivo {} do bucket {}.'.format(key, bucket))
        raise e
```


## Preparação do banco de dados

A criação do database e das tabelas foram realizadas em python utilizando médodos da biblioteca boto3, aplicando scripts/querys no formato do Athena (baseado em Presto) para fontes de dados em CSV (forma de pagamentos e empresas de taxi) e JSON (viagens).

Os logs e execuções do Athena são armazenados na pasta "s3://ytbd-nyctaxi/athena/".

As fórmulas e scripts estão no notebook **nyc-taxi-trips-database**.

**Database: nyctaxi**

**Tabela payment:** payment_type, payment_lookup

**Tabela vendor:** vendor_id, name, address, city, state, zip, country, contact, current_contact

**Tabela trips:** vendor_id, pickup_datetime, dropoff_datetime, passenger_count,trip_distance, pickup_longitude, pickup_latitude, rate_code, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, surcharge, tip_amount, tolls_amount, total_amount


## Resultados

As fórmulas e scripts estão no notebook **nyc-taxi-trips-analises**.


Distância média percorrida por viagens com no máximo 2 passageiros: **2.66**
```
SELECT avg(t.trip_distance) as average_distance
FROM nyctaxi.trips t
WHERE t.passenger_count <= 2
```

Tempo médio em minutos de corridas no fim de semana (sábado e domingo): **8.74**
```
SELECT
avg(date_diff('second',
  from_iso8601_timestamp(pickup_datetime),
  from_iso8601_timestamp(dropoff_datetime)
))/60.0 minutes
FROM nyctaxi.trips
WHERE day_of_week(from_iso8601_timestamp(pickup_datetime)) IN (6,7)
```

Maiores vendors em valor total arrecadado:
```
SELECT v.name, sum(t.total_amount) as total_amount
FROM nyctaxi.trips t
INNER JOIN nyctaxi.vendor v
ON v.vendor_id = t.vendor_id
GROUP BY v.vendor_id, v.name
ORDER BY total_amount DESC
LIMIT 3
```
![Maiores Vendors](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_maiores_vendors.png)

Quantidades de corridas por mês pagas em dinheiro:
```
SELECT substr(t.pickup_datetime, 1, 7) as pickup_year_month, count(1) trips_cash_count
FROM nyctaxi.trips t
INNER JOIN nyctaxi.payment p
ON p.payment_type = t.payment_type
WHERE upper(p.payment_lookup) = 'CASH'
GROUP BY substr(t.pickup_datetime, 1, 7)
ORDER BY trips_cash_count
```
![Corridas dinheiro](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_corridas_dinheiro.png)

Quantidades de corridas com gorjeta por dia dos últimos 3 meses com dados de 2012:
```
SELECT MAX(cast(from_iso8601_timestamp(pickup_datetime) as date)) data FROM nyctaxi.trips
```
```
SELECT cast(from_iso8601_timestamp(pickup_datetime) as date) as pickup_date, count(*) trip_count
FROM nyctaxi.trips
WHERE tip_amount > 0
AND cast(from_iso8601_timestamp(pickup_datetime) as date) >= date_add('month',-3,date('{max_data}'))
GROUP BY cast(from_iso8601_timestamp(pickup_datetime) as date)
ORDER BY pickup_date
```
![Corridas gorjeta](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_corridas_gorjeta.png)


Mapa de viagens em 2010:
```
SELECT vendor_id, cast(from_iso8601_timestamp(pickup_datetime) as date) as pickup_date,
pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
round(pickup_longitude,1) lon_ini, round(pickup_latitude,1) lat_ini,
round(dropoff_longitude,1) lon_fim, round(dropoff_latitude,1) lat_fim
FROM nyctaxi.trips
WHERE year(from_iso8601_timestamp(pickup_datetime)) = 2010
LIMIT 1000
```
```
SELECT round(pickup_longitude,2) lon_ini, round(pickup_latitude,2) lat_ini, sum(1) trip_count
FROM nyctaxi.trips
WHERE year(from_iso8601_timestamp(pickup_datetime)) = 2010
GROUP BY round(pickup_longitude,2), round(pickup_latitude,2)
```
![Mapa Viagem1](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_mapa_viagem1.png)
![Mapa Viagem2](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_mapa_viagem2.png)
