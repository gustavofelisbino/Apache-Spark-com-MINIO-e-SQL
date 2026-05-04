# 03 — DML no Delta Lake

Demonstra INSERT, UPDATE e DELETE nas tabelas Delta do bucket bronze.

## INSERT — via merge

```python
dt.alias("t").merge(novos.alias("s"), "t.id = s.id").whenNotMatchedInsertAll().execute()
```

## UPDATE — condicional

```python
dt_apolice.update(
    condition=col("cobertura") == "Básica",
    set={"valor_premio": col("valor_premio") * lit(1.15)}
)
```

## DELETE — condicional

```python
dt_sinistro.delete(condition=col("status") == "Recusado")
dt_carro.delete(condition=col("ano") < lit(2018))
```

## Time Travel

```python
DeltaTable.forPath(spark, dest).history().select("version", "timestamp", "operation").show()
```
