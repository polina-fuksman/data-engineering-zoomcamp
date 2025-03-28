# Project attempt 1

## Disclaimer

Project was made as simple as possible. 
I wanted to test my understanding of how the pipeline can be created on static, one-file data. 
In the second attempt, I want to make another more complicated pipeline.

## Stack

On this pipeline, I used:
Docker
Jupyter Notebook (data exploration), 
Kestra (data ingestion into the data lake and data warehouse), 
dbt (data models), 
GCP (data lake, data warehouse, BI tool): Google Storage, BigQuery, and Looker.

Python and SQL

## Step 1. Dataset and data exploration

Dataset was downloaded from Kaggle LINK.  
The dataset represents the payment data, with parameters:  
step,  
..  
fraud

Data exploration step, made in the notebook (payment_data_exploration.ipynb), showed that:

- no outliers
- data clean and nice
- one numerical data
- several categorical parameters 

## Step 2. Data workflow with Kestra

01_gcp_kv.yaml and 02_gcp_setup.yaml for gcp
03_gcp_payments.yaml workflow itself

## Step 3. Data modeling with dbt

stg_bs.sql - staging table

payment_data.sql
volume_by_category.sql
volume_by_fraud.sql
volume_by_merchant.sql

# Step 4. Visualization

Using tables:
payment_data
volume_by_category
volume_by_fraud
volume_by_merchant

![alt text](image.png)