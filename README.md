# NYC TAXI TRIPS

Esse projeto realiza a análise de base de dados de corrida de taxi em Nova York no período de 2009 a 2012, considerando caracteŕisticas como tempo entre corridas, existência de gorjetas, valores em dinheiro, volume de corridas por mês ou dia.

![Taxi](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/taxi.png)


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
- nyctaxi-lambda-s3-curated: identifica inserção de novos arquivos na pasta curated, transforma o arquivo para a pasta lakehouse (nesse momento apenas copia) e mantém o arquivo também na pasta curated

Neste momento o provisionamento da infraestrutura ainda não foi versionalizado e realizado automaticamente com ferramentas como o TerraForm (esta é uma próxima etapa deste projeto).


## Visualizações dos dados

As visualizações de dados foram desenvolvidas utilizando o Power BI (NYC-Taxi.pbix). As consultas com os dados foram exportadas do Redshift em formato csv (nyctaxi-dadoscsvpowerbi.zip) pois ocorreu um problema no conector ODBC do Power BI com o Redshift, assim não foi possível fazer as consultas diretamente.

Nesta primeira visualização temos informações gerais:
- Distância média percorrida por viagens com no máximo 2 passageiros
- Tempo médio em minutos de corridas no fim de semana (sábado e domingo)
- Os 3 maiores fornecedores em valor total arrecadado
- Quantidades de corridas pagas em dinheiro por mês e sua distribuição mensal em histograma
- Quantidades de gorjetas diárias (visão geral e visão mais detalhada por dia)

![Painel Geral](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/nyctaxi-painel1.png)

Neste mapa temos uma visão geral das maiores localidades de início e fim das viagens. Obseva-se grande concentração em Manhattan, Brooklyn, New York, Aeroportos John Kennedy e LaGuardia.

![Painel Mapas](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/nyctaxi-painel2.PNG)

É interessante observar que as viagens possuem um volume maior de destino que de origem na proximidade do Harlem, Washington Heights e Brooklyn.

![Painel Mapas](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/nyctaxi-painel2-origem.png)

![Painel Mapas](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/nyctaxi-painel2-destino.png)

Nessa última visão percebe-se uma concentração de destino é maior nos arredores de New York e há um volume interessante de origens mais distantes de New York.

![Painel Mapas](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/nyctaxi-painel3.PNG)


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

Foi aplicada a role **nyctaxi-role-s3-raw** com as políticas: AmazonS3FullAccess

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


## Função nyctaxi-lambda-s3-curated

A função identifica inserção de novos arquivos na pasta curated, transforma o arquivo para a pasta lakehouse (nesse momento apenas copia) e mantém o arquivo também na pasta curated.

Esta função posteriormente poderia ser incrementada para transformação dos dados apurado para o lakehouse.

**Role nyctaxi-role-s3-curated**

Foi aplicada a role **nyctaxi-role-s3-curated** com as políticas: AmazonS3FullAccess

```
{
  "Version": "2012-10-17",
  "Id": "default",
  "Statement": [
    {
      "Sid": "lambda-032fb73c-f07b-4c12-a1a0-3dd98c8d9764",
      "Effect": "Allow",
      "Principal": {
        "Service": "s3.amazonaws.com"
      },
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:716124686595:function:nyctaxi-lambda-s3-curated",
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

    # Copia para lakehouse zone
    try:

        response = s3.get_object(Bucket=bucket, Key=key)
        obj_content_type = response['ContentType']

        key_lkh = key.replace('nyctaxi-curated','nyctaxi-lakehouse')

        # Novo arquivo identificado
        print('Novo arquivo identificado: {}/{}'.format(bucket, key))

        # Copia arquivo para área de lakehouse z
        print('Copia arquivo para área de lakehouse zone: {}/{}'.format(bucket, key_lkh))
        copy_source = { 'Bucket' : bucket, 'Key' : key }
        s3.copy(copy_source, bucket, key_lkh)

        return bucket, key, obj_content_type

    except Exception as e:

        print(e)
        print('Erro ao recuperar / traballhar arquivo {} do bucket {}.'.format(key, bucket))
        raise e
```

## Banco de dados

O serviço do Amazon Redshift gerencia todo o trabalho de um data warehouse com opções de busca de dados a partir de arquivos do S3 (nesta solução buscando os dados a partir do diretório **nyctaxi-lakehouse**).

**Cluster:** nyctaxi-dw (dc2.large, 1 node, 160 GB)

**Role:** nyctaxi-role-redshift (AmazonRedshiftQueryEditor, AmazonRedshiftFullAccess, AmazonS3ReadOnlyAccess)

**Database:** nyctaxi

**Tabela payment:**
- subdiretório: payment
- tipo de arquivo: csv
- campos: payment_type, payment_lookup

**Tabela vendor:**
- subdiretório: vendor
- tipo de arquivo: csv
- campos: vendor_id, name, address, city, state, zip, country, contact, current_contact

**Tabela trips:**
- subdiretório: trips
- tipo de arquivo: json
- campos: vendor_id, pickup_datetime, dropoff_datetime, passenger_count,trip_distance, pickup_longitude, pickup_latitude, rate_code, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, surcharge, tip_amount, tolls_amount, total_amount

**Scripts criação schema e tabelas**

Nesta etapa os scripts foram criados manualmente utilizando o Query Editor do Redshift, mapeando as tabelas no schema **nyctaxi** para melhor organização.

A melhor opção é criar de forma automática para melhor organização e versionamento, mas no momento falta conhecimento e prática suficientes para fazê-lo até conclusão da versão atual.

```

create schema nyctaxi;

create table nyctaxi.vendor (
  vendor_id varchar(3) not null,
  name varchar(100),
  address varchar(100),
  city varchar(100),
  state varchar(2),
  zip int,
  country varchar(50),
  contact varchar(100),
  current varchar(3)
);

create table nyctaxi.payment (
  payment_type varchar(20) not null,
  payment_lookup varchar(50)
);

create table nyctaxi.trips (
  vendor_id varchar(100),
  pickup_datetime varchar(100),
  dropoff_datetime varchar(100),
  passenger_count int,
  trip_distance decimal(10,2),
  pickup_longitude decimal(10,6),
  pickup_latitude decimal(10,6),
  rate_code varchar(100),
  store_and_fwd_flag varchar(100),
  dropoff_longitude decimal(10,6),
  dropoff_latitude decimal(10,6),
  payment_type varchar(100),
  fare_amount decimal(10,2),
  surcharge decimal(10,2),
  tip_amount decimal(10,2),
  tolls_amount decimal(10,2),
  total_amount decimal(10,2)
);

```

**Scripts carga de dados**

Os dados foram carregados no Redshift utilizando o command **COPY** no Query Editor, mepeando os dados a partir das respectivas pastas no lakehouse com seus formatos e tipos de arquivos.

```

copy nyctaxi.vendor from 's3://ytbd-nyctaxi/nyctaxi-lakehouse/vendor/' 
credentials 'aws_iam_role=arn:aws:iam::716124686595:role/nyctaxi-role-redshift' 
csv delimiter ',' region 'us-east-1' bzip2 IGNOREHEADER 1;

copy nyctaxi.payment from 's3://ytbd-nyctaxi/nyctaxi-lakehouse/payment/' 
credentials 'aws_iam_role=arn:aws:iam::716124686595:role/nyctaxi-role-redshift' 
csv delimiter ',' region 'us-east-1' bzip2 IGNOREHEADER 2;

copy nyctaxi.trips from 's3://ytbd-nyctaxi/nyctaxi-lakehouse/trips/' 
credentials 'aws_iam_role=arn:aws:iam::716124686595:role/nyctaxi-role-redshift' 
region 'us-east-1' json 'auto' bzip2;

```

## Consultas referência visualizações


Distância média percorrida por viagens com no máximo 2 passageiros: **2.65**
```
SELECT avg(trip_distance) as average_distance
FROM nyctaxi.trips
WHERE passenger_count <= 2
```

Tempo médio em minutos de corridas no fim de semana (sábado e domingo): **8.73**
```
SELECT
avg(date_diff('second',
  cast(left(replace(pickup_datetime,'T',' '),19) AS DATETIME),
  cast(left(replace(dropoff_datetime,'T',' '),19) AS DATETIME)
))/60.0 minutes
FROM nyctaxi.trips
WHERE date_part(dow, cast(left(pickup_datetime,10) AS date)) IN (6.0,0.0)
```

Maiores vendors em valor total arrecadado:
- **Creative Mobile Technologies, LLC - 19542120.60**
- **VeriFone Inc - 19036953.84**
- **Dependable Driver Service, Inc - 2714003.52**
```
SELECT v.name, sum(t.total_amount) as total_amount
FROM nyctaxi.trips t
INNER JOIN nyctaxi.vendor v
ON v.vendor_id = t.vendor_id
GROUP BY v.vendor_id, v.name
ORDER BY total_amount DESC
LIMIT 3
```

Quantidades de corridas por mês pagas em dinheiro:
```
SELECT substring(t.pickup_datetime,1,7) as pickup_year_month, count(1) trips_cash_count
FROM nyctaxi.trips t
INNER JOIN nyctaxi.payment p
ON p.payment_type = t.payment_type
WHERE upper(p.payment_lookup) = 'CASH'
GROUP BY substring(t.pickup_datetime,1,7)
ORDER BY trips_cash_count
```

Quantidades de corridas com gorjeta por dia dos últimos 3 meses com dados de 2012:
```
SELECT cast(substring(pickup_datetime,1,10) as date) as pickup_date, count(*) trip_count
FROM nyctaxi.trips
WHERE tip_amount > 0
AND cast(substring(pickup_datetime,1,10) as date) >=
dateadd(month,-3,(
  select max(cast(substring(pickup_datetime,1,10) as date))
  from nyctaxi.trips
  where substring(pickup_datetime,1,4) = '2012'
))
GROUP BY cast(substring(pickup_datetime,1,10) as date)
ORDER BY pickup_date
```

Mapa de viagens em 2010:
```
SELECT vendor_id, cast(substring(pickup_datetime,1,10) as date) as pickup_date,
pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude,
round(pickup_longitude,1) lon_ini, round(pickup_latitude,1) lat_ini,
round(dropoff_longitude,1) lon_fim, round(dropoff_latitude,1) lat_fim
FROM nyctaxi.trips
WHERE substring(pickup_datetime,1,4) = '2010'
limit 1000
```
```
SELECT round(pickup_longitude,2) lon_ini, round(pickup_latitude,2) lat_ini, sum(1) trip_count
FROM nyctaxi.trips
WHERE substring(pickup_datetime,1,4) = '2010'
AND round(pickup_longitude,4) <> '0.0000'
GROUP BY round(pickup_longitude,2), round(pickup_latitude,2)
ORDER BY trip_count DESC
limit 1000
```
```
SELECT round(dropoff_longitude,2) lon_fim, round(dropoff_latitude,2) lat_fim, sum(1) trip_count
FROM nyctaxi.trips
WHERE substring(pickup_datetime,1,4) = '2010'
AND round(pickup_longitude,4) <> '0.0000'
GROUP BY round(dropoff_longitude,2), round(dropoff_latitude,2)
ORDER BY trip_count DESC
limit 1000
```
