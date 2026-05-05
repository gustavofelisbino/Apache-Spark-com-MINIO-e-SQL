# 03 — DML no Delta Lake { .hero-title }

## O que este notebook faz { .reveal }

Demonstra as operações de DML que o Delta Lake oferece nativamente sobre os dados da camada bronze: **INSERT** (via MERGE), **UPDATE**, **DELETE** e **Time Travel**. Cada operação gera um novo *commit* no `_delta_log/`, mantendo o histórico completo da tabela.

---

## Operações cobertas { .reveal }

<div class="grid cards reveal" markdown>

-   :material-plus-box:{ .lg .middle .accent-spark } &nbsp; __INSERT (MERGE)__

    ---

    Adiciona novos registros sem duplicar os já existentes

-   :material-pencil:{ .lg .middle .accent-delta } &nbsp; __UPDATE__

    ---

    Atualiza colunas com base em uma condição

-   :material-delete:{ .lg .middle .accent-sqlserver } &nbsp; __DELETE__

    ---

    Remove linhas que satisfazem uma condição

-   :material-history:{ .lg .middle .accent-bronze } &nbsp; __Time Travel__

    ---

    Consulta versões anteriores via `versionAsOf` ou `timestampAsOf`

</div>

---

## Setup { .reveal }

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

BRONZE = "s3a://bronze"

dt_apolice  = DeltaTable.forPath(spark, f"{BRONZE}/apolice")
dt_sinistro = DeltaTable.forPath(spark, f"{BRONZE}/sinistro")
dt_carro    = DeltaTable.forPath(spark, f"{BRONZE}/carro")
```

---

## INSERT — via MERGE { .reveal }

O `MERGE` é mais robusto que um `append` porque evita duplicatas — se o `id` já existir, o registro é ignorado.

```python
novos = spark.createDataFrame(
    [(11, 5, 7, "Total", 3500.00, "2025-01-01", "2026-01-01")],
    ["id_apolice", "id_cliente", "id_carro",
     "cobertura", "valor_premio", "data_inicio", "data_fim"],
)

(
    dt_apolice.alias("t")
    .merge(novos.alias("s"), "t.id_apolice = s.id_apolice")
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

## UPDATE — reajuste condicional { .reveal }

Aplica um aumento de 15% no prêmio das apólices com cobertura *Básica*:

```python
dt_apolice.update(
    condition = col("cobertura") == "Básica",
    set       = {"valor_premio": col("valor_premio") * lit(1.15)},
)
```

A versão equivalente em SQL Spark:

```python
spark.sql("""
    UPDATE delta.`s3a://bronze/apolice`
    SET valor_premio = valor_premio * 1.15
    WHERE cobertura = 'Básica'
""")
```

---

## DELETE — limpeza condicional { .reveal }

Remove sinistros com status *Recusado* e veículos anteriores a 2018:

```python
dt_sinistro.delete(condition = col("status") == "Recusado")
dt_carro.delete(condition    = col("ano") < lit(2018))
```

---

## Time Travel { .reveal }

Cada operação acima criou uma nova versão. Dá para inspecionar o histórico e ler qualquer versão anterior:

=== "Histórico de operações"

    ```python
    (
        DeltaTable.forPath(spark, f"{BRONZE}/apolice")
        .history()
        .select("version", "timestamp", "operation", "operationMetrics")
        .show(truncate=False)
    )
    ```

    Saída típica:

    ```
    +-------+-------------------+---------+
    |version|timestamp          |operation|
    +-------+-------------------+---------+
    |2      |2025-04-21 18:45:11|UPDATE   |
    |1      |2025-04-21 18:44:53|MERGE    |
    |0      |2025-04-21 18:30:02|WRITE    |
    +-------+-------------------+---------+
    ```

=== "Ler versão específica"

    ```python
    df_v0 = (
        spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(f"{BRONZE}/apolice")
    )
    df_v0.show()
    ```

=== "Ler por timestamp"

    ```python
    df_ontem = (
        spark.read.format("delta")
        .option("timestampAsOf", "2025-04-20 23:59:59")
        .load(f"{BRONZE}/apolice")
    )
    ```

---

## Verificação final { .reveal }

```python
for tabela in ["apolice", "sinistro", "carro"]:
    versoes = (
        DeltaTable.forPath(spark, f"{BRONZE}/{tabela}")
        .history()
        .count()
    )
    total = spark.read.format("delta").load(f"{BRONZE}/{tabela}").count()
    print(f"{tabela:10s} → {total} linhas, {versoes} versões")
```

!!! success "Pronto"
    O pipeline foi executado de ponta a ponta: SQL Server → CSV → Delta Lake, com DML transacional e histórico completo. Os dados na camada bronze já podem alimentar uma camada *silver* ou *gold* posteriormente.
