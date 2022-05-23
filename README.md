# Crops-Data-Ingestion

## Objective

The main objective of this project is to produce an output excel file based on 3 crop-related input files:
- Crop Protection Market Size (Market Size.xlsx)
- Crop Protection Treated Acres (Treated Acres.csv)
- USDA Planted Acres (an excel file extracted from zip)

Given that the 3 source file data occur at different levels of granularity, the data will be aggregated and summarized when loading to the output file.

## Tools

The tools used to complete this project are:
- Python (pandas, numpy, profiler etc. see requirements.txt file)
- Docker/Docker-Compose
- Apache Airflow
- Unix bash commands
- Jupyter Notebook

![](images/crops-architecture.png)
