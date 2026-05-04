# 00 — Setup SQL Server

Cria o banco `seguradora` no SQL Server e importa os dados dos CSVs da pasta `data/`.

## Tabelas criadas

`estado`, `regiao`, `municipio`, `endereco`, `cliente`, `telefone`, `marca`, `modelo`, `carro`, `apolice`, `sinistro`

## Código principal

```python
cursor.execute(f"CREATE DATABASE {DB}")
# DDL de cada tabela...
# Loop de importação via csv.DictReader + cursor.execute INSERT
```
