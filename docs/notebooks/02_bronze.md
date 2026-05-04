# 02 — Camada Bronze: landing-zone → Delta Lake

Le os arquivos CSV da landing-zone com Apache Spark e salva no bucket bronze no formato Delta Lake.

## SparkSession

Configurada com suporte a Delta Lake e MinIO via S3A.

## Código principal

```python
df = spark.read.option("header", "true").option("inferSchema", "true").csv(src)
df.write.format("delta").mode("overwrite").save(dest)
count = spark.read.format("delta").load(dest).count()
```

## Resultado

Cada tabela gera uma pasta Delta com arquivos Parquet e `_delta_log/`.
