# Trabalho 2 — Apache Spark com MinIO e SQL Server

Pipeline de dados com extração do SQL Server, armazenamento no MinIO e conversão para Delta Lake.

## Fluxo

```
SQL Server (seguradora)
        │
        │  pyodbc (boto3)
        ▼
MinIO: landing-zone  ← CSVs por tabela
        │
        │  Apache Spark
        ▼
MinIO: bronze        ← Delta Lake (ACID, Time Travel, DML)
```

## Banco de dados

O banco `seguradora` contém 11 tabelas de uma seguradora de automóveis:

| Tabela | Descrição |
|---|---|
| estado | Estados brasileiros |
| regiao | Regiões do Brasil |
| municipio | Municípios |
| endereco | Endereços dos clientes |
| cliente | Clientes da seguradora |
| telefone | Telefones dos clientes |
| marca | Marcas de veículos |
| modelo | Modelos de veículos |
| carro | Veículos segurados |
| apolice | Apólices de seguro |
| sinistro | Sinistros registrados |

## Observação

Este trabalho **não** implementa Apache Iceberg. A conversão para Iceberg é requisito exclusivo do Trabalho 1.
