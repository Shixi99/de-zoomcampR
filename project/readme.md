# Real Estate Project

## Problem Statement

The aim of the project is to create a business-friendly dashboard to collect all the data from the real estate sale website and analyze the market. The result of the project is an interactive tableau dashboard in which we can easily see a bunch of parameters. 
Our problem was we cannot see how the market is going. We want to get answers to the questions below.

- Does the price is increasing or decreasing?
- What types of properties (house/villa or apartment) are sold mostly?
- How many room houses are sold mostly?
- Which size houses are sold mostly?
- Which regions are more active than others?
- Which are mortgage eligible?
- Which are sold with document?
- Are they new or old building?


## Tech Stack

First, data was scraped from the website and stored in **AWS S3**. Then, I used **Prefect** for orchestration, ensuring that the project's various components ran smoothly. The **Prefect deployment* was built with **Docker** infrastructure, which made deployment a breeze.

To create resources in AWS, I utilized **Terraform**. This made it easy to manage and deploy the resources needed for the project.

**Parquet files** stored in **S3** were crawled by **AWS Crawler** in **AWS Glue** and stored in a **AWS Glue Catalog** table. This made it simple to query the data using **AWS Athena**. For analytics, **Tableau** was connected to **Athena** and a dashboard was created.

The diagrams below were sketched in Exceldraw.

<img  height='500' src='https://github.com/Shixi99/de-zoomcampR/blob/main/project/data%20flow%20charts/flow1.png'/>

Alternatively, I also used **Dremio** (data lakehouse) to query data directly **AWS S3** and I connect **Dremio** to **Tableau** for analytics.

<img  height='500' src='https://github.com/Shixi99/de-zoomcampR/blob/main/project/data%20flow%20charts/flow%20with%20dremio.png'/>


## Dashboard

https://public.tableau.com/shared/XR9Y4MRY7?:display_count=n&:origin=viz_share_link

