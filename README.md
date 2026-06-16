# Netflix-Azure-Data-Engineering-Project


---

# Overview

This project demonstrates an **End-to-End Azure Data Engineering Pipeline** built using Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Databricks Workflows and Delta Live Tables.

The project follows **Medallion Architecture (Bronze → Silver → Gold)** and implements several production-grade concepts used in modern Data Engineering projects.

The pipeline performs:

* Dynamic ingestion from GitHub using Azure Data Factory
* Incremental data ingestion using Databricks Auto Loader
* Parameterized Databricks notebooks
* Dynamic notebook orchestration using Databricks Workflows
* Conditional execution using If Else logic
* Gold Layer creation using Delta Live Tables (DLT)
* Data Quality checks using DLT Expectations

---

# Key Features

* Dynamic Azure Data Factory Pipelines

* Parameterized Datasets

* Web Activity

* Validation Activity

* ForEach Activity

* Incremental Data Ingestion

* Databricks Auto Loader

* Parameterized Databricks Notebooks

* Databricks Widgets

* Databricks Workflows

* Task Values

* If Else Conditions

* Delta Live Tables

* Data Quality Expectations

* Streaming Tables

* Medallion Architecture

* GitHub Integration

---

# Architecture

The project follows Medallion Architecture.

```text
GitHub Lookup Files

            │

            ▼

Azure Data Factory

(Web Activity)

(Set Variable)

(Validation Activity)

(ForEach Activity)

(Copy Activity)

            │

            ▼

Azure Data Lake Storage Gen2


raw

bronze

silver

gold


            ▲

            │


Databricks Auto Loader

Incremental Ingestion


            │

            ▼


Silver Transformations

PySpark

Widgets

Parameterized Notebooks


            │

            ▼


Databricks Workflows

ForEach

If Else

Task Values


            │

            ▼


Delta Live Tables


            │

            ▼


Gold Layer

Business Ready Tables


            │

            ▼


Power BI
```

---

# Tech Stack

| Service                      | Purpose                    |
| ---------------------------- | -------------------------- |
| Azure Data Factory           | Dynamic Data Ingestion     |
| Azure Data Lake Storage Gen2 | Storage Layer              |
| Azure Databricks             | Data Processing            |
| Databricks Auto Loader       | Incremental Data Ingestion |
| Databricks Workflows         | Notebook Orchestration     |
| Delta Live Tables            | Gold Layer                 |
| PySpark                      | Data Transformation        |
| GitHub                       | Version Control            |
|                              |                            |

---

# Repository Structure

```text
Netflix-Azure-Data-Engineering-Project

│
├── adf/
│   ├── dataset/
│   ├── linkedService/
│   ├── pipeline/
│   └── factory/

│
├── notebooks/
│   ├── 1_Autoloader.py
│   ├── 2_silver.py
│   ├── 3_lookupNotebook.py
│   ├── 4_Silver.py
│   ├── 5_lookupNotebook.py
│   ├── 6_falsenotebook.py
│   └── 7_DLT_Notebook.py

│
├── screenshots/
│   ├── adf_pipeline.png
│   ├── silver_workflow.png
│   ├── if_else_workflow.png
│   └── dlt_pipeline.png

│
└── README.md
```

---

# Source Data

This project uses Netflix Movies and TV Shows dataset.

Two sources are used.

---

## Source 1

### GitHub Lookup Files

Lookup files:

* netflix_cast.csv

* netflix_category.csv

* netflix_countries.csv

* netflix_directors.csv

These files are dynamically ingested using Azure Data Factory.

---

## Source 2

### Netflix Titles Dataset

Contains:

* show_id

* type

* title

* director

* cast

* country

* date_added

* release_year

* rating

* duration

* listed_in

* description

This file is incrementally ingested using Databricks Auto Loader.

---

# Data Lake Architecture

Azure Data Lake Storage Gen2 is divided into four containers.

```text
raw

bronze

silver

gold
```

---

## Raw Layer

Stores incoming Netflix source files.

Features:

* Landing Zone

* Raw CSV Files

* No Transformations

---

## Bronze Layer

Stores raw ingested data.

Features:

* Incremental Ingestion

* Auto Loader

* Schema Evolution

* Delta Format

---

## Silver Layer

Stores cleansed and transformed data.

Features:

* Null Handling

* Data Type Casting

* String Transformations

* Window Functions

* Business Rules

* Delta Tables

---

## Gold Layer

Stores business-ready datasets.

Features:

* Delta Live Tables

* Streaming Tables

* Views

* Data Quality Rules

* Analytics Ready Data

---

# Azure Data Factory

ADF is used as the ingestion and orchestration layer.

The pipeline dynamically copies lookup files from GitHub into ADLS Gen2 Bronze Layer.

Features:

* HTTP Linked Service

* Parameterized Datasets

* Web Activity

* Validation Activity

* ForEach Activity

* Copy Activity

---

# ADF Pipeline

Pipeline Flow:

```text
GithubMetadata

↓

Set Variable

↓

ValidationGithub

↓

ForAllTheFiles

↓

Copy Github Data
```

---

## Dynamic Parameter

The pipeline uses an array parameter.

```json
[
 {
  "folder_name":"netflix_cast",
  "file_name":"netflix_cast.csv"
 },

 {
  "folder_name":"netflix_category",
  "file_name":"netflix_category.csv"
 },

 {
  "folder_name":"netflix_countries",
  "file_name":"netflix_countries.csv"
 },

 {
  "folder_name":"netflix_directors",
  "file_name":"netflix_directors.csv"
 }
]
```

Using a single pipeline, all lookup files are dynamically copied to ADLS.

---

# Databricks Bronze Layer

Notebook:

```text
1_Autoloader.py
```

Bronze Layer uses Databricks Auto Loader for incremental ingestion.

Features:

* Incremental Data Ingestion

* Schema Evolution

* Checkpointing

* Streaming Ingestion

* Automatic File Discovery

---

## Auto Loader

Uses:

```python
spark.readStream

.format("cloudFiles")

.option("cloudFiles.format","csv")
```

The notebook maintains:

* Schema Location

* Checkpoint Location

* Source Path

* Sink Path

which ensures fault tolerant incremental ingestion.

---

# Databricks Silver Layer

Silver Layer consists of:

1. Lookup Tables

2. Netflix Titles Transformations

---

## Lookup Tables

Notebook:

```text
2_silver.py
```

Uses:

```python
dbutils.widgets
```

to create:

* sourcefolder

* targetfolder

Parameters.

The notebook:

* Reads Bronze Delta Files

* Uses Dynamic Paths

* Writes Delta Files

* Stores Output in Silver Layer

---

## Netflix Titles Transformation

Notebook:

```text
4_Silver.py
```

Transformations performed:

### Null Handling

Missing values are replaced.

Example:

```python
fillna()
```

---

### Type Casting

Columns are converted to:

* IntegerType

* StringType

for efficient storage.

---

### String Transformations

Short title extraction:

```python
split(title,':')
```

Rating standardization:

```python
split(rating,'-')
```

---

### Business Logic

Movie

```text
Movie → 1
```

TV Show

```text
TV Show → 2
```

Stored as:

```text
type_flag
```

---

### Window Functions

Ranking is created using:

```python
dense_rank()
```

to rank content by duration.

---

### SQL Operations

The notebook also demonstrates:

* Temporary Views

* Global Temporary Views

* Spark SQL

---

# Databricks Workflows

Databricks Workflows orchestrate notebooks dynamically.

---

## Dynamic Workflow

Workflow:

```text
Lookup_Locations

↓

SilverNotebook_iteration
```

The lookup notebook generates an array.

The array is returned using:

```python
dbutils.jobs.taskValues.set()
```

and consumed dynamically by downstream notebooks.

---

## Conditional Workflow

Workflow:

```text
WeekdayLookup

↓

IfWeekDay

↓

TRUE

↓

SilverMasterData



FALSE

↓

FalseNotebook
```

This demonstrates:

* If Else Conditions

* Dynamic Execution

* Notebook Orchestration

* Task Values

* Conditional Processing

---

# Delta Live Tables (DLT)

Notebook:

```text
7_DLT_Notebook.py
```

DLT Pipeline:

---

## Gold Tables

The following Gold Layer tables are created:

* gold_netflixcast

* gold_netflixcategory

* gold_netflixcountries

* gold_netflixdirectors

* gold_stg_netflixtitles

* gold_trns_netflixtitles

* gold_netflixtitles

---

