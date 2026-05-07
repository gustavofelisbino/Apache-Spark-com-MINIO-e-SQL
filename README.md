# Apache Spark com MinIO e SQL Server

**Trabalho 2 — Arquitetura de Dados**
Autores: **Gustavo Felisbino e Lucas Oliverio**

Pipeline de dados: extrai um banco SQL Server, grava CSV em MinIO (camada *landing-zone*), e converte para Delta Lake (camada *bronze*), com operações DML transacionais (INSERT/UPDATE/DELETE) e Time Travel.

> Este README é o **manual operacional**: instruções de execução, dependências, versões, troubleshooting. Para a explicação conceitual do projeto (arquitetura, decisões de design, conteúdo dos notebooks com diagramas), consulte a documentação MkDocs publicada em **https://gustavofelisbino.github.io/Apache-Spark-com-MINIO-e-SQL/**.

---

## 1. Pré-requisitos

| Ferramenta | Versão mínima testada | Notas |
|---|---|---|
| **Docker Desktop** | 4.30+ | Em **Apple Silicon (M1/M2/M3)** habilite **Settings → General → Use Rosetta for x86_64/amd64 emulation** |
| **Git** | 2.40+ | Para clonar o repositório |
| **Navegador** | Chrome / Safari / Firefox | Para Jupyter Lab e MinIO Console |

> **Nada precisa ser instalado no host além do Docker.** Java, Python, Spark, drivers ODBC e bibliotecas vivem todos dentro dos containers.

---

## 2. Stack e versões exatas

### Containers / runtimes

| Componente | Versão | Imagem base |
|---|---|---|
| **SQL Server** | 2022 (latest) | `mcr.microsoft.com/mssql/server:2022-latest` (linux/amd64) |
| **MinIO** | latest | `minio/minio:latest` |
| **Jupyter Lab + PySpark** | Spark 3.5.0 | `jupyter/pyspark-notebook:spark-3.5.0` (estendida — ver §6) |
| **Python (no container)** | 3.11.x | herdado da imagem base |
| **Java (no container)** | OpenJDK 17 | herdado da imagem base |
| **Apache Spark** | 3.5.0 | bundled com Hadoop client 3.3.4, Scala 2.12 |

### JARs adicionados ao Spark (`/usr/local/spark/jars/`)

| JAR | Versão | Para que serve |
|---|---|---|
| `hadoop-aws` | 3.3.4 | Driver `S3AFileSystem` — habilita o protocolo `s3a://` |
| `aws-java-sdk-bundle` | 1.12.262 | SDK AWS exigido pelo `hadoop-aws` |
| `delta-spark_2.12` | 3.2.0 | Extensão Delta Lake para Spark SQL (DML, MERGE, history) |
| `delta-storage` | 3.2.0 | Camada de escrita do `_delta_log/` |

> **Atenção a compatibilidade:** Spark 3.5.0 usa Hadoop client 3.3.4, então `hadoop-aws` precisa ser exatamente 3.3.4. Delta 3.2.0 é a versão oficial para Spark 3.5.x. Mismatches quebram em runtime.

### Drivers de sistema instalados (Linux dentro do container)

| Pacote | Versão | Por quê |
|---|---|---|
| `msodbcsql18` | última estável do repo Microsoft | Driver ODBC 18 para SQL Server, exigido pelo `pyodbc` |
| `unixodbc-dev` | do apt do Ubuntu 22.04 | Biblioteca C que o `pyodbc` linka |

### Bibliotecas Python (instaladas via `pip` no container)

| Pacote | Versão | Usado em |
|---|---|---|
| `pyodbc` | 5.1.0 | Notebooks 00 e 01 (conexão SQL Server) |
| `python-dotenv` | 1.0.1 | Todos os notebooks (carregar `.env`) |
| `boto3` | 1.34.131 | Notebooks 01 e 02 (cliente S3 para MinIO) |
| `delta-spark` | 3.2.0 | Notebooks 02 e 03 (API `DeltaTable`) |

---

## 3. Estrutura do repositório

```
.
├── data/                              # CSVs com a carga inicial do banco
│   ├── apolice.csv  carro.csv  cliente.csv  endereco.csv
│   ├── estado.csv   marca.csv  modelo.csv   municipio.csv
│   ├── regiao.csv   sinistro.csv  telefone.csv
├── notebooks/                         # 4 notebooks Jupyter (executar em ordem)
│   ├── 00_setup_sqlserver.ipynb
│   ├── 01_sqlserver_to_minio_csv.ipynb
│   ├── 02_csv_to_delta.ipynb
│   └── 03_dml_delta.ipynb
├── docs/                              # Fonte da documentação MkDocs
│   ├── stylesheets/extra.css
│   ├── notebooks/
│   ├── arquitetura.md
│   └── index.md
├── .env.example                       # Template de variáveis de ambiente
├── .gitignore
├── .python-version                    # Versão Python (uv/pyenv)
├── Dockerfile.jupyter                 # Imagem custom do Jupyter (ODBC + JARs)
├── docker-compose.yml                 # Orquestração dos 3 serviços
├── mkdocs.yml                         # Configuração da documentação
├── pyproject.toml                     # Metadados do projeto
└── README.md
```

---

## 4. Banco de dados — `seguradora`

Sistema de seguro de automóveis com **11 tabelas relacionais** e ~120 registros de carga inicial.

```
regiao → estado → municipio → endereco → cliente → telefone
                                            │
                                         apolice → sinistro
                                            │
            marca → modelo → carro ─────────┘
```

| Tabela | Registros | Descrição |
|---|---:|---|
| `regiao` | 5 | Regiões do Brasil |
| `estado` | 15 | Estados brasileiros |
| `municipio` | 10 | Municípios |
| `endereco` | 10 | Endereços dos clientes |
| `cliente` | 10 | Clientes da seguradora |
| `telefone` | 10 | Telefones de contato |
| `marca` | 8 | Marcas de veículos |
| `modelo` | 15 | Modelos de veículos |
| `carro` | 10 | Veículos segurados |
| `apolice` | 10 | Apólices de seguro |
| `sinistro` | 8 | Sinistros registrados |

---

## 5. Instalação e execução — passo a passo

### 5.1. Clonar o repositório

```bash
git clone https://github.com/gustavofelisbino/Apache-Spark-com-MINIO-e-SQL.git
cd Apache-Spark-com-MINIO-e-SQL
```

### 5.2. Configurar variáveis de ambiente

```bash
cp .env.example .env
```

Conteúdo do `.env` (valores padrão funcionam com o `docker-compose.yml`):

```dotenv
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_LANDING_BUCKET=landing-zone
MINIO_BRONZE_BUCKET=bronze

SQLSERVER_HOST=sqlserver
SQLSERVER_PORT=1433
SQLSERVER_DB=seguradora
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=Senha@1234
```

> **Importante:** `MINIO_ENDPOINT` usa o hostname `minio` (resolvido pela rede Docker `lakehouse`). O mesmo vale para `SQLSERVER_HOST=sqlserver`. Não troque por `localhost` — os containers se enxergam pelo nome do serviço.

### 5.3. Build e subida dos containers

**Primeira execução** (faz a build da imagem custom do Jupyter — leva ~3-5 min na primeira vez):

```bash
docker-compose up -d --build
```

**Execuções seguintes** (reusa a imagem em cache):

```bash
docker-compose up -d
```

Aguarde **~45 segundos** para o SQL Server terminar de inicializar, então confira:

```bash
docker-compose ps
```

Os 3 serviços (`sqlserver`, `minio`, `jupyter`) devem estar com status `running`.

### 5.4. Validar a infraestrutura

```bash
# 1. SQL Server respondendo
docker exec sqlserver /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'Senha@1234' -C -Q "SELECT @@VERSION"

# 2. JARs do Spark estão no classpath
docker exec jupyter ls /usr/local/spark/jars/ | grep -iE 'aws|delta'
# esperado: 4 arquivos (hadoop-aws, aws-java-sdk-bundle, delta-spark, delta-storage)

# 3. Driver ODBC instalado
docker exec jupyter python -c "import pyodbc; print(pyodbc.drivers())"
# esperado: ['ODBC Driver 18 for SQL Server']
```

### 5.5. Acessar o Jupyter Lab

Pegue o token gerado a cada inicialização:

```bash
docker logs jupyter 2>&1 | grep "127.0.0.1:8888/lab?token"
```

Cole a URL completa no navegador. Em caso de erro 403, abra em janela anônima (cookie antigo).

### 5.6. Executar os notebooks **em ordem**

| # | Arquivo | O que faz | Dependência |
|---|---|---|---|
| 00 | `00_setup_sqlserver.ipynb` | DROP+CREATE banco `seguradora`, DDL das 11 tabelas, INSERT a partir dos CSVs | SQL Server up |
| 01 | `01_sqlserver_to_minio_csv.ipynb` | Para cada tabela: `SELECT *` → CSV em memória → `put_object` no bucket `landing-zone` (cria se não existir) | Notebook 00 ok |
| 02 | `02_csv_to_delta.ipynb` | Cria SparkSession com Delta + S3A, garante bucket `bronze`, lê cada CSV e grava como Delta | Notebook 01 ok |
| 03 | `03_dml_delta.ipynb` | INSERT via MERGE, UPDATE condicional, DELETE condicional, leitura do `dt.history()` | Notebook 02 ok |

**Procedimento para cada notebook:**

1. Abrir o notebook
2. Menu **Kernel → Restart Kernel** (libera memória; importante entre o 02 e o 03)
3. Menu **Run → Run All Cells**

> O notebook 02 demora ~30 segundos na primeira inicialização do SparkContext.

### 5.7. Validar resultado final no MinIO

Abra http://localhost:9001 (login `minioadmin` / `minioadmin`).

- Bucket **`landing-zone`** com 11 pastas, cada uma com um `<tabela>.csv`.
- Bucket **`bronze`** com 11 pastas Delta. Dentro de cada uma:
  - arquivos `part-*.snappy.parquet`
  - pasta `_delta_log/` com 1+ arquivos `00000000000000000000.json`, `...000001.json`, etc. (cada um é um commit Delta)

---

## 6. Imagem customizada do Jupyter (`Dockerfile.jupyter`)

A imagem oficial `jupyter/pyspark-notebook:spark-3.5.0` **não vem** com:
- driver ODBC da Microsoft
- JARs do `hadoop-aws` / `aws-java-sdk-bundle`
- JARs do Delta Lake
- bibliotecas Python `pyodbc`, `boto3`, `delta-spark`

O [Dockerfile.jupyter](Dockerfile.jupyter) estende a imagem base adicionando tudo isso. Resumo do que ele faz:

```dockerfile
FROM jupyter/pyspark-notebook:spark-3.5.0
USER root

# 1. Adiciona o repo da Microsoft e instala msodbcsql18 + unixodbc-dev
# 2. Baixa 4 JARs (hadoop-aws, aws-sdk-bundle, delta-spark, delta-storage)
#    diretamente para /usr/local/spark/jars/
# 3. Instala pyodbc, python-dotenv, boto3, delta-spark via pip

USER ${NB_UID}
```

A primeira build leva 3–5 minutos (download dos JARs ~200 MB + apt install). Depois fica cacheada localmente. **Use sempre `docker-compose up -d --build` quando o Dockerfile mudar.**

---

## 7. Interfaces disponíveis

| Serviço | URL | Credenciais |
|---|---|---|
| **Jupyter Lab** | http://localhost:8888 | token nos logs do container |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |
| **MinIO API S3** | http://localhost:9000 | `minioadmin` / `minioadmin` |
| **SQL Server** | `localhost:1433` (cliente externo) | `sa` / `Senha@1234` |

---

## 8. Reset completo (recomeçar do zero)

```bash
# 1. Para tudo e remove volumes (apaga banco e buckets)
docker-compose down -v

# 2. Limpa outputs dos notebooks (opcional — deixa "virgem" para demo)
python3 - <<'EOF'
import json, glob
for path in glob.glob("notebooks/*.ipynb"):
    with open(path) as f:
        nb = json.load(f)
    for cell in nb["cells"]:
        if cell.get("cell_type") == "code":
            cell["outputs"] = []
            cell["execution_count"] = None
    with open(path, "w") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
EOF

# 3. Sobe novamente (sem --build, reusa a imagem do Jupyter)
docker-compose up -d
sleep 45
```

---

## 9. Troubleshooting

### `ModuleNotFoundError: No module named 'pyodbc'`
A imagem do Jupyter está sem as libs custom. **Solução:** rebuild forçado.
```bash
docker-compose down
docker-compose up -d --build
```

### `java.lang.ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem`
Os JARs do Hadoop AWS não estão no classpath. **Diagnóstico:**
```bash
docker exec jupyter ls /usr/local/spark/jars/ | grep -iE 'aws|delta'
```
Se vier vazio, rebuild a imagem (sem cache):
```bash
docker-compose down
docker-compose build --no-cache jupyter
docker-compose up -d
```

### `com.amazonaws.services.s3.model.AmazonS3Exception: NoSuchBucket: bronze`
O bucket `bronze` não existe ainda. O notebook 02 já tem uma célula que cria automaticamente — execute-a antes do loop. Alternativamente, crie via CLI do MinIO:
```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/bronze
```

### SQL Server: `Login timeout expired`
SQL Server demora para inicializar (~30s em x86, ~45s em ARM). Aguarde e rode novamente. Validar:
```bash
docker logs sqlserver | grep "ready for client connections"
```

### Jupyter retorna `403 Forbidden` no navegador
Cookie de sessão antigo. Abra em **janela anônima** com a URL completa contendo `?token=...`.

### `! sqlserver  The requested image's platform (linux/amd64) does not match` (Apple Silicon)
Aviso esperado em Mac M1/M2/M3 — o `docker-compose.yml` declara `platform: linux/amd64` para emulação via Rosetta. Não é erro. Para performance aceitável, garanta que **Rosetta esteja habilitado** em Docker Desktop → Settings → General.

### Build do Dockerfile falha em `curl https://repo1.maven.org/...`
Problema de rede ao baixar JARs. Tente novamente ou use proxy. Verifique conectividade:
```bash
docker run --rm curlimages/curl curl -I https://repo1.maven.org/maven2/
```

### `dt.history()` retorna apenas a versão 0 após rodar o notebook 03
O notebook 03 não foi executado completamente. Cada operação DML (MERGE, UPDATE, DELETE) gera uma versão. Execute todas as células e rode o último bloco de novo.

---

## 10. Documentação conceitual (MkDocs)

A documentação detalhada — diagramas, explicação de cada notebook, decisões de arquitetura — está publicada em:

**https://gustavofelisbino.github.io/Apache-Spark-com-MINIO-e-SQL/**

### Executar localmente

Com [uv](https://docs.astral.sh/uv/) instalado (não precisa criar venv):

```bash
uvx --from mkdocs-material mkdocs serve
```

Ou via `pip`:

```bash
pip install mkdocs-material
mkdocs serve
```

Acesse http://127.0.0.1:8000.

### Publicar

```bash
uvx --from mkdocs-material mkdocs gh-deploy --force
```

---

## 11. Notas sobre escopo

- **Não implementa Apache Iceberg** — esse é tema do Trabalho 1. Aqui usa-se apenas Delta Lake.
- **Pipeline para até a camada Bronze** — não há *Silver* nem *Gold*. O foco é a transição de banco relacional → data lake transacional.
- **Volume de dados é didático** (~120 registros) — a arquitetura é a mesma que rodaria com terabytes, apenas mudaria a escala de cluster.

---

## 12. Referências

- [Apache Spark 3.5 — Documentação](https://spark.apache.org/docs/3.5.0/)
- [Delta Lake 3.2 — Documentação](https://docs.delta.io/3.2.0/)
- [MinIO — Documentação](https://min.io/docs/minio/linux/index.html)
- [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)
- [Hadoop-AWS Module — Apache Hadoop](https://hadoop.apache.org/docs/r3.3.4/hadoop-aws/tools/hadoop-aws/index.html)
- [Repositório de referência — `jlsilva01/spark-delta-minio-sqlserver`](https://github.com/jlsilva01/spark-delta-minio-sqlserver)
