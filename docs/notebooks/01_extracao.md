# 01 — Extração: SQL Server → landing-zone

Lê todas as tabelas do SQL Server via `pyodbc` e faz upload para o MinIO como CSV usando `boto3`.

## Origem → Destino

| Origem | Destino |
|---|---|
| `seguradora.<tabela>` | `landing-zone/<tabela>/<tabela>.csv` |

## Código principal

```python
cursor.execute(f"SELECT * FROM {table}")
buf = io.StringIO()
writer = csv.writer(buf)
writer.writerow(cols)
writer.writerows(rows)
s3.put_object(Bucket=LANDING, Key=key, Body=io.BytesIO(payload))
```
