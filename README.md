# END TO END DATA QUALITY PIPELINE

# Data Architecture
![image](https://github.com/user-attachments/assets/4a9f4dc2-02e5-4a47-9bc3-f2b775f797b2)

## Details Architecture

- Google Sheets: used as the primary data source.
- Airbyte: a tool dedicated to ELT (Extract & Load) operations.
- Airflow and Astronomer: a combined solution for deploying data pipelines, integrating the open-source orchestration framework Airflow.
- Snowflake: a storage platform used to store data at each critical stage of the pipeline.
- Soda Core and Soda Cloud: solutions employed to conduct tests and monitor data quality throughout the pipeline.
- Power BI: a data visualization tool for effectively representing insights in a clear and meaningful way.

### Prerequisites:
To successfully complete this project, it’s necessary to install the following tools in advance:

- <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/docker/docker-original-wordmark.svg" alt="docker" width="40" height="40"/> DOCKER → Download link
- ASTRO CLI → Download link
- AIRBYTE CLOUD ACCOUNT (Free 14 days) → Link to Airbyte
- SODA CLOUD ACCOUNT (Free 30 days) → Link to Soda
- SNOWFLAKE ACCOUNT → Link to Snowflake
And of course, Python 3.X because the code won’t write itself.

### Launch this  project ...
Check on medium article for a full tutorial  : <a href = 'https://medium.com/@nisaacemmanuel/end-to-end-batch-data-quality-pipeline-with-astronomer-airbyte-snowflake-soda-ca7a70f15300'>my medium article link</a>

### Business Intelligence Parts
The main objective of data pipeline sometiemes, is to serve dashboard for decision making.
My Power BI dashboard look like  : 

![image](https://github.com/user-attachments/assets/61e7f6d8-a173-4c45-bb4a-0f42e83402de)

![image](https://github.com/user-attachments/assets/a123c5f5-671e-40e4-8d66-22b30f556f33)


