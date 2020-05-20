# NYC TAXI TRIPS

Análise de base de dados de corrida de taxi em Nova York no período de 2009 a 2012, considerando caracteŕisticas como tempo entre corridas, existência de gorjetas, valores em dinheiro, volume de corridas por mês ou dia.


## Preparação de dados

Os dados foram extraídos da base disponibilizada, compactados em formato BZ2 (bzip), armazenados no bucket storage da AWS S3 e consumidos a partir do AWS Athena (este realiza a leitura a partir do storage). Ao armazenar o dado compactado reduz-se a tranferência de dados e custo de  consumo deste dado inclusive no AWS Athena.

A compactação foi realizada via command no Linux (arquivos na pasta local "data"):

- bzip2 -c data/data-payment_lookup-csv.csv > data/payment.bz2
- bzip2 -c data/data-vendor_lookup-csv.csv > data/vendor.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2009-json_corrigido.json > data/trips2009.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2010-json_corrigido.json > data/trips2010.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2011-json_corrigido.json > data/trips2011.bz2
- bzip2 -c data/data-sample_data-nyctaxi-trips-2012-json_corrigido.json > data/trips2012.bz2

A criação e atualização no bucket foi realizada utilizando comandos AWS CLI (AWS Command Line Interface, autenticado localmente):

- aws s3api create-bucket --bucket ytbd-nyctaxi
- aws s3 cp data/payment.bz2 s3://ytbd-nyctaxi/payment/payment.bz2
- aws s3 cp data/vendor.bz2 s3://ytbd-nyctaxi/vendor/vendor.bz2
- aws s3 cp data/trips2009.bz2 s3://ytbd-nyctaxi/trips/trips2009.bz2
- aws s3 cp data/trips2010.bz2 s3://ytbd-nyctaxi/trips/trips2010.bz2
- aws s3 cp data/trips2011.bz2 s3://ytbd-nyctaxi/trips/trips2011.bz2
- aws s3 cp data/trips2012.bz2 s3://ytbd-nyctaxi/trips/trips2012.bz2


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
SELECT round(pickup_longitude,1) lon_ini, round(pickup_latitude,1) lat_ini, sum(1) trip_count
FROM nyctaxi.trips
WHERE year(from_iso8601_timestamp(pickup_datetime)) = 2010
GROUP BY round(pickup_longitude,1), round(pickup_latitude,1)
```
![Mapa Viagem1](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_mapa_viagem1.png)
![Mapa Viagem2](https://github.com/barcelosyussif/nyc-taxi-trips/blob/master/resultado_mapa_viagem2.png)
