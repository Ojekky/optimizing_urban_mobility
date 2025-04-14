# Optimizing Urban Mobility: A Data Pipeline for Capital Bikeshare Trip & Membership Analysis

## Project Overview
This project builds a complete data engineering pipeline to analyze and optimize bike-sharing operations in Washington DC using Capital Bikeshare trip data. The system ingests, processes, and analyzes bike trip records to uncover usage patterns, fleet performance, and member behavior to support operational decisions and member engagement strategies.

## Problem Being Solved
**Capital Bikeshare** must balance bike availability, station demand, and user satisfaction. However, without efficient data processing, it’s difficult to:
1. Understand trip duration trends.
2. Identify high-demand stations.
3. Track how members vs. casual riders use the system.
4. Improve bike fleet utilization.

## 📌 Project Objective
The primary objective of this project is to build an end-to-end Data Engineering pipeline for analyzing bike-sharing data, leveraging modern cloud and open-source tools. The goal is to efficiently ingest, process, store, and visualize bike trip data to uncover valuable insights about station performance, user behavior, and operational patterns.

✅ Technical Objectives:
1. Infrastructure Deployment using **Terraform**
- Provision and manage cloud resources on **Google Cloud Platform (GCP)** in an automated, scalable, and reproducible manner using **Terraform**.

2. Data Ingestion using **Kestra**
- Implement a workflow orchestration system with Kestra to automate the ingestion of bike-share data from public sources into a **Google Cloud Storage (GCS) bucket**.

3. Data Processing with **Apache Spark**
- Utilize Apache Spark for batch processing of the ingested data.
- Perform data transformations and combine multiple raw datasets using Spark SQL.
- Save the processed and cleaned data as Parquet files into the designated **GCS bucket** for further analytics.

4. Data Warehousing with **BigQuery**
- Load the processed Parquet files from the GCS bucket into Google BigQuery.
- Create partitioned and clustered tables for optimized query performance.
- Design and run SQL queries to derive business insights such as station demand trends, member vs. casual usage patterns, and trip distance analytics.

5. Data Visualization with **Looker Studio**
- Connect Looker Studio to the BigQuery data warehouse.
- Develop an interactive and visually appealing dashboard to present key metrics and insights including:
    - Total rides over time
    - Peak usage hours and days
    - Station utilization analysis
    - Membership type behavior comparison
    - Trip distance and duration distribution
    - Popular routes and geo-mapped station activity

## 🏗️ Architecture and Highlights
![image](https://github.com/user-attachments/assets/90673fce-a7b8-4d43-8222-2bb440a81d8a)

# Terraform (Set Up)
##📌 Overview
This document outlines the Infrastructure as Code (IaC) approach using Terraform to provision and manage the cloud infrastructure required for the Capital Bikeshare Data Pipeline on Google Cloud Platform (GCP).

## Setup
### 🛠️ Prerequisites
- [Terraform](https://developer.hashicorp.com/terraform/downloads)
- [VScode](https://code.visualstudio.com/download) 
- A GCP Service Account with the required permissions

Used steps in [DE-Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/README.md)
## :movie_camera: Introduction Terraform: Concepts and Overview, a primer

[![](https://markdown-videos-api.jorgenkh.no/youtube/s2bOYDCKl_M)](https://youtu.be/s2bOYDCKl_M&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=11)

* [Companion Notes](1_terraform_gcp)

## :movie_camera: Terraform Basics: Simple one file Terraform Deployment

[![](https://markdown-videos-api.jorgenkh.no/youtube/Y2ux7gq3Z0o)](https://youtu.be/Y2ux7gq3Z0o&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=12)

* [Companion Notes](1_terraform_gcp)

## :movie_camera: Deployment with a Variables File

[![](https://markdown-videos-api.jorgenkh.no/youtube/PBi0hHjLftk)](https://youtu.be/PBi0hHjLftk&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=13)

* [Companion Notes](1_terraform_gcp) 

### 🚀 Deploying Infrastructure
```bash
terraform init.
terraform applty
```

## 🔄 Ingestion with Kestra
In this project, Kestra is used as the workflow orchestration tool to automate and manage the ingestion of raw Capital Bikeshare trip data from external sources into Google Cloud Storage (GCS). Kestra workflows ensure reliable, scheduled, and traceable data ingestion pipelines.

### 🛠️ Prerequisites
- [Docker](https://www.docker.com/products/docker-desktop/) & [Docker Compose](https://docs.docker.com/compose/install/)
- [Install Kestra on GCP Virtual machine with ​Cloud ​S​Q​L and ​G​C​S(Linked terraform bucket and dataset)](https://kestra.io/docs/installation/gcp-vm) 
- A GCP Service Account with the required permissions

### 📌 Start Kestra
Run the following command to start Kestra using Docker Compose:

```bash
docker-compose up -d
```
After Kestra UI is loaded, we can run two following flows:

### 🔑 set_kv: Configures Environment Variables

The flow ([WDC_bike_gcp_kv.yaml](kestra/workflow/WDC_bike_gcp_kv.yaml)) configure the following project variables:
- `gcp_project_id`
- `gcp_location`
- `gcp_bucket_name`

### ⚡ data_load_gcs: Fetches data, saves as CSV, and uploads to GCS

The [wdc_bike_data_gcp.yaml](kestra/workflow/wdc_bike_data_gcp.yaml) flow orchestrates the entire ingestion pipeline:

- Fetches data from the [Capitalbike Share history data](https://s3.amazonaws.com/capitalbikeshare-data/index.html) in Zip files, then extract it to CSV files.
- Uploads the CSVs to the specified GCS bucket.
- Purges temporary files to keep the workflow clean.



![Using LockerStudio ](image.png)
![2](image-1.png)
![3](image-2.png)
![4](image-3.png)





This project is created as part of the [Data Engineering Zoomcamp 2025 course](https://github.com/DataTalksClub/data-engineering-zoomcamp). Special thanks to [DataTalkClub](https://github.com/DataTalksClub) for the learning opportunity.
