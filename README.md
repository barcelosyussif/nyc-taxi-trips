# NYC TAXI TRIPS

Análise de base de dados de corrida de taxi em Nova York no período de 2009 a 2012, considerando caracteŕisticas como tempo entre corridas, existência de gorjetas, valores em dinheiro, volume de corridas por mês ou dia.

## Preparação de dados


bzip2 -c data/data-payment_lookup-csv.csv > data/payment.bz2

bzip2 -c data/data-vendor_lookup-csv.csv > data/vendor.bz2

bzip2 -c data/data-sample_data-nyctaxi-trips-2009-json_corrigido.json > data/trips2009.bz2

bzip2 -c data/data-sample_data-nyctaxi-trips-2010-json_corrigido.json > data/trips2010.bz2

bzip2 -c data/data-sample_data-nyctaxi-trips-2011-json_corrigido.json > data/trips2011.bz2

bzip2 -c data/data-sample_data-nyctaxi-trips-2012-json_corrigido.json > data/trips2012.bz2

aws s3 cp data/payment.bz2 s3://ytbd-datasprints/payment/payment.bz2
aws s3 cp data/vendor.bz2 s3://ytbd-datasprints/vendor/vendor.bz2
aws s3 cp data/trips2009.bz2 s3://ytbd-datasprints/trips/trips2009.bz2
aws s3 cp data/trips2010.bz2 s3://ytbd-datasprints/trips/trips2010.bz2
aws s3 cp data/trips2011.bz2 s3://ytbd-datasprints/trips/trips2011.bz2
aws s3 cp data/trips2012.bz2 s3://ytbd-datasprints/trips/trips2012.bz2
