# Arquitetura

## Visão geral

```
┌──────────────────────┐
│  SQL Server 2022     │
│  banco: seguradora   │
│  11 tabelas          │
└──────────┬───────────┘
           │ pyodbc + boto3
           ▼
┌──────────────────────┐
│  MinIO               │
│  bucket: landing-zone│
│  formato: CSV        │
└──────────┬───────────┘
           │ Apache Spark 3.5
           ▼
┌──────────────────────┐
│  MinIO               │
│  bucket: bronze      │
│  formato: Delta Lake │
│  INSERT/UPDATE/DELETE│
└──────────────────────┘
```

## Containers Docker

| Container | Porta | Função |
|---|---|---|
| sqlserver | 1433 | Banco de dados fonte |
| minio | 9000 / 9001 | Object storage |
| jupyter | 8888 | Ambiente de notebooks |
