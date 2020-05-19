import os
import time
import boto3
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px

client = boto3.client('athena')


def executar_query(query):
    """Executa consulta no Athena retornando o QueryExecutionId"""

    response = client.start_query_execution(
        QueryString = query,
        ResultConfiguration= { 'OutputLocation': 's3://ytbd-datasprints/athena/' }
    )
    return response['QueryExecutionId']


def verificar_query(query_id):
    """Aguarda conclusão de query retornando o status de sucesso ou erro"""

    query_status = 'RUNNING'
    while query_status == 'QUEUED' or query_status == 'RUNNING':
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        time.sleep(1)

    return query_status


def listar_query(query_id):
    """Retorna lista de valores da query onde a primeira linha é o cabeçalho"""

    lst_resultado = []

    paginacao = client.get_paginator('get_query_results')
    lst_pagina = paginacao.paginate(
        QueryExecutionId = query_id,
        PaginationConfig = { 'PageSize': 1000 }
    )

    for pagina in lst_pagina:
        for linha in pagina['ResultSet']['Rows']:
            lst_resultado.append([x['VarCharValue'] for x in linha['Data']])

    return lst_resultado


# Resultado distancia media até 2 passageiros
query = 'SELECT avg(t.trip_distance) as average_distance ' \
        'FROM nyctaxi.trips t ' \
        'WHERE t.passenger_count <= 2'
query_id = executar_query(query)
query_status = verificar_query(query_id)

if query_status == 'SUCCEEDED':
    lst = listar_query(query_id)
    distancia_media = lst[1][0]
    print('Distância média percorrida por viagens com no máximo 2 passageiros: {}'.format(distancia_media))
else:
    print('Distância média percorrida por viagens com no máximo 2 passageiros: erro ao executar consulta)')


# Resultado 3 maiores vendors em dinheiro
query = 'SELECT v.name, sum(t.total_amount) as total_amount ' \
        'FROM nyctaxi.trips t ' \
        'INNER JOIN nyctaxi.vendor v ' \
        'ON v.vendor_id = t.vendor_id ' \
        'GROUP BY v.vendor_id, v.name ' \
        'ORDER BY total_amount DESC ' \
        'LIMIT 3'
query_id = executar_query(query)
query_status = verificar_query(query_id)

if query_status == 'SUCCEEDED':
    lst = listar_query(query_id)
    maiores_vendors = ' / '.join('{} ({})'.format(i[0], i[1]) for i in lst[1:4])
    print('3 maiores vendors em total de dinheiro arrecadado: {}'.format(maiores_vendors))
else:
    print('3 maiores vendors em total de dinheiro arrecadado: erro ao executar consulta')


# Resultado quantidades de corridas por mês pagas em dinheiro
query = 'SELECT substr(t.pickup_datetime, 1, 7) as pickup_year_month, count(1) trips_cash_count ' \
        'FROM nyctaxi.trips t ' \
        'WHERE upper(payment_type) = \'CASH\'' \
        'GROUP BY substr(t.pickup_datetime, 1, 7) ' \
        'ORDER BY trips_cash_count'
query_id = executar_query(query)
query_status = verificar_query(query_id)

if query_status == 'SUCCEEDED':
    lst = listar_query(query_id)
    df = pd.DataFrame(lst[1:], columns=lst[0])

    print('Histograma distribuição mensal corridas pagas em dinheiro:')
    qtd_bins = 12
    n, bins, patches = plt.hist(df['trips_cash_count'].tolist(), bins=qtd_bins, facecolor='green', alpha=0.5)
    plt.xticks(bins)
    plt.show()


# Resultado quantidades de corridas dos últimos 3 meses com gorjeta
query = 'SELECT cast(from_iso8601_timestamp(pickup_datetime) as date) as pickup_date, count(*) trip_count ' \
        'FROM nyctaxi.trips ' \
        'WHERE tip_amount > 0 ' \
        'AND cast(from_iso8601_timestamp(pickup_datetime) as date) >= ' \
        '    date_add(\'month\',-3,(SELECT MAX(cast(from_iso8601_timestamp(pickup_datetime) as date)) FROM nyctaxi.trips))' \
        'GROUP BY cast(from_iso8601_timestamp(pickup_datetime) as date) ' \
        'ORDER BY pickup_date'
query_id = executar_query(query)
query_status = verificar_query(query_id)

if query_status == 'SUCCEEDED':
    lst = listar_query(query_id)
    df = pd.DataFrame(lst[1:], columns=lst[0])
    fig = px.line(df, x='pickup_date', y='trip_count')
    fig.show()
