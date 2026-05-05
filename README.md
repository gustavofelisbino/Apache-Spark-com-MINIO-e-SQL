# Apache Spark com MinIO e SQL Server

**Trabalho 2 — Arquitetura de Dados**  
Aluno: **Gustavo Felisbino e Lucas Oliverio**

---

## 📋 Sobre o Projeto

Este repositório implementa um pipeline de dados completo seguindo a **arquitetura Medallion**, utilizando **Apache Spark (PySpark)** para extração e transformação, **SQL Server** como banco de dados fonte, **MinIO** como object storage compatível com S3 e **Delta Lake** como formato de tabela aberta (*open table format*) com suporte a transações ACID.

O pipeline percorre duas camadas:

| Camada | Bucket | Formato | Descrição |
|---|---|---|---|
| **Landing Zone** | `landing-zone` | CSV | Dados brutos extraídos do SQL Server via `pyodbc` e `boto3` |
| **Bronze** | `bronze` | Delta Lake | Dados convertidos para formato transacional com DML nativo |

> **Observação:** Este trabalho **não** implementa Apache Iceberg. A conversão para Iceberg é requisito exclusivo do Trabalho 1.

---

## Pre-requisitos

Antes de começar, certifique-se de ter instalado:

| Ferramenta | Versão mínima | Download |
|---|---|---|
| **Docker Desktop** | 4.x | [docker.com](https://www.docker.com/products/docker-desktop/) |
| **Git** | 2.x | [git-scm.com](https://git-scm.com/) |

> O Docker irá provisionar automaticamente o SQL Server, MinIO e Jupyter com PySpark — não é necessário instalar Java, Python ou Spark localmente.

---

## Banco de Dados — Seguradora

O banco `seguradora` é um sistema de **seguro de automóveis** com 11 tabelas relacionais. Os dados de carga estão nos arquivos CSV da pasta `data/`.

```
estado ──── municipio ──── endereco ──── cliente ──── telefone
                                              │
                                           apolice ──── sinistro
                                              │
marca ──── modelo ──── carro ────────────────┘
```

| Tabela | Registros | Descrição |
|---|---|---|
| `estado` | 15 | Estados brasileiros |
| `regiao` | 5 | Regiões do Brasil |
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

## Configurando o Ambiente

### 1. Clone o repositório

```bash
git clone https://github.com/gustavofelisbino/<repo>.git
cd <repo>
```

### 2. Configure o `.env`

```bash
cp .env.example .env
```

O arquivo `.env` contém as credenciais dos serviços. Os valores padrão já funcionam com o `docker-compose.yml`:

```env
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

### 3. Suba os containers

```bash
docker-compose up -d
```

Aguarde cerca de **30 segundos** para o SQL Server inicializar completamente. Verifique o status:

```bash
docker-compose ps
```

---

## Executando os Notebooks

### Acesse o Jupyter Lab

Abra [http://localhost:8888](http://localhost:8888) no navegador e navegue até a pasta `work/notebooks/`.

### Execute os notebooks em ordem

| # | Arquivo | Descrição |
|---|---|---|
| `00` | `00_setup_sqlserver.ipynb` | Cria o banco `seguradora`, todas as tabelas e importa os CSVs |
| `01` | `01_sqlserver_to_minio_csv.ipynb` | Extrai todas as tabelas do SQL Server e grava como CSV no bucket `landing-zone` |
| `02` | `02_csv_to_delta.ipynb` | Lê os CSVs com Apache Spark e converte para Delta Lake no bucket `bronze` |
| `03` | `03_dml_delta.ipynb` | Executa INSERT, UPDATE e DELETE nas tabelas Delta com verificação via Time Travel |

> **Dica:** Execute as células em ordem (`Shift + Enter`). O notebook `02` pode demorar alguns segundos pois o Spark precisa inicializar.

---

## Estrutura do Repositório

```
.
├── data/                              # CSVs com dados do banco seguradora
│   ├── apolice.csv
│   ├── carro.csv
│   ├── cliente.csv
│   ├── endereco.csv
│   ├── estado.csv
│   ├── marca.csv
│   ├── modelo.csv
│   ├── municipio.csv
│   ├── regiao.csv
│   ├── sinistro.csv
│   └── telefone.csv
├── docs/                              # Documentação MkDocs
│   ├── notebooks/
│   │   ├── 00_setup.md
│   │   ├── 01_extracao.md
│   │   ├── 02_bronze.md
│   │   └── 03_dml.md
│   ├── arquitetura.md
│   └── index.md
├── notebooks/                         # Jupyter notebooks PySpark
│   ├── 00_setup_sqlserver.ipynb
│   ├── 01_sqlserver_to_minio_csv.ipynb
│   ├── 02_csv_to_delta.ipynb
│   └── 03_dml_delta.ipynb
├── .env.example                       # Variáveis de ambiente (modelo)
├── .gitignore
├── .python-version
├── docker-compose.yml                 # Orquestração dos serviços
├── mkdocs.yml                         # Configuração do MkDocs
├── pyproject.toml                     # Dependências do projeto
└── README.md
```

---

## Interfaces de Monitoramento

| Serviço | URL | Usuário | Senha |
|---|---|---|---|
| **Jupyter Lab** | [http://localhost:8888](http://localhost:8888) | — | — |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` | `minioadmin` |

---

## Arquitetura do Pipeline

```
┌──────────────────────┐
│    SQL Server 2022   │
│   banco: seguradora  │         pyodbc + boto3
│     11 tabelas       │ ─────────────────────────────────────────┐
└──────────────────────┘                                          │
                                                                  ▼
                                                  ┌──────────────────────────┐
                                                  │  MinIO: landing-zone     │
                                                  │  formato: CSV            │
                                                  │  (dados brutos)          │
                                                  └─────────────┬────────────┘
                                                                │
                                                                │  Apache Spark 3.5
                                                                ▼
                                                  ┌──────────────────────────┐
                                                  │  MinIO: bronze           │
                                                  │  formato: Delta Lake     │
                                                  │  ACID · Time Travel      │
                                                  │  INSERT · UPDATE · DELETE│
                                                  └──────────────────────────┘
```

---

## Versões das Dependências Principais

| Biblioteca / Serviço | Versão |
|---|---|
| Python | 3.11+ |
| Apache Spark | 3.5.0 |
| Delta Lake | 3.x |
| SQL Server | 2022 |
| MinIO | latest |
| boto3 | 1.34+ |
| python-dotenv | 1.0+ |
| pyodbc | 5.0+ |

---

## Documentação (MkDocs)

### Executando localmente

```bash
pip install mkdocs mkdocs-material
mkdocs serve
```

Acesse [http://127.0.0.1:8000](http://127.0.0.1:8000) para visualizar.

---

## Referências

- [Documentação oficial do Apache Spark](https://spark.apache.org/docs/latest/)
- [Documentação do Delta Lake](https://docs.delta.io/latest/index.html)
- [Documentação do MinIO](https://min.io/docs/minio/linux/index.html)
- [Repositório modelo — spark-delta-minio-sqlserver](https://github.com/jlsilva01/spark-delta-minio-sqlserver)
- [Canal DataWay BR — YouTube](https://www.youtube.com/@DataWayBR)
