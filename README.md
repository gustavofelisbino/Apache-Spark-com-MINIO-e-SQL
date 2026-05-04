# Trabalho 2 — Apache Spark com MinIO e SQL Server

Pipeline de dados com extração do SQL Server, armazenamento no MinIO e conversão para Delta Lake.

## Tecnologias

- Apache Spark 3.5
- SQL Server 2022
- MinIO (Object Storage S3-compatible)
- Delta Lake
- Python 3.11
- Docker

## Estrutura

```
.
├── data/                          # CSVs com dados do banco seguradora
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
├── docs/                          # Documentação MkDocs
│   ├── notebooks/
│   │   ├── 00_setup.md
│   │   ├── 01_extracao.md
│   │   ├── 02_bronze.md
│   │   └── 03_dml.md
│   ├── arquitetura.md
│   └── index.md
├── notebooks/                     # Jupyter notebooks
│   ├── 00_setup_sqlserver.ipynb
│   ├── 01_sqlserver_to_minio_csv.ipynb
│   ├── 02_csv_to_delta.ipynb
│   └── 03_dml_delta.ipynb
├── .env.example
├── .gitignore
├── .python-version
├── docker-compose.yml
├── mkdocs.yml
└── pyproject.toml
```

## Como executar

### 1. Clone o repositório

```bash
git clone https://github.com/gustavofelisbino/<repo>.git
cd <repo>
```

### 2. Configure o `.env`

```bash
cp .env.example .env
```

### 3. Suba os containers

```bash
docker-compose up -d
```

### 4. Execute os notebooks em ordem

| Notebook | Descrição |
|---|---|
| `00_setup_sqlserver.ipynb` | Cria o banco `seguradora` e importa os CSVs |
| `01_sqlserver_to_minio_csv.ipynb` | Extrai todas as tabelas para o bucket `landing-zone` em CSV |
| `02_csv_to_delta.ipynb` | Converte os CSVs para Delta Lake no bucket `bronze` |
| `03_dml_delta.ipynb` | Executa INSERT, UPDATE e DELETE nas tabelas Delta |

## Interfaces

| Serviço | URL | Credenciais |
|---|---|---|
| Jupyter Lab | http://localhost:8888 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

## Documentação

```bash
pip install mkdocs mkdocs-material
mkdocs serve
```

Acesse: http://localhost:8000
