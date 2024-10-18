# Data Engineering Pipeline for E-commerce Sales Analysis

## Introduction

Welcome to the **Data Engineering Pipeline for E-commerce Sales Analysis** project. This pipeline is designed to automate the ingestion, transformation, storage, and analysis of sales data for an e-commerce startup. It provides robust handling of raw and unformatted data, generates insightful reports, and leverages advanced AI capabilities to offer quick insights—all while mimicking the architecture of AWS services using local tools.

## Table of Contents

- [Features](#features)
- [Architecture and AWS Parallels](#architecture-and-aws-parallels)
- [Unformatted Input Data Simulation](#unformatted-input-data-simulation)
- [Data Pipeline Flow](#data-pipeline-flow)
- [Installation and Setup](#installation-and-setup)
  - [Prerequisites](#prerequisites)
  - [Installation Steps](#installation-steps)
- [Usage](#usage)
  - [Running the Application](#running-the-application)
  - [Accessing the Dashboard](#accessing-the-dashboard)
  - [Uploading Data](#uploading-data)
- [Reports and Analytics](#reports-and-analytics)
- [Technical Decisions and Considerations](#technical-decisions-and-considerations)
- [Logging and Monitoring](#logging-and-monitoring)
- [Error Handling and Data Validation](#error-handling-and-data-validation)
- [Automation and Scalability](#automation-and-scalability)
- [Conclusion](#conclusion)
- [Contact Information](#contact-information)
- [Acknowledgments](#acknowledgments)

## Features

- **Automated Data Pipeline**: Seamless ingestion, transformation, and loading of sales data.
- **Robust Data Handling**: Processes malformed and inconsistent CSV files with varying delimiters and formats.
- **Incremental Data Loading**: Upsert logic to handle new and existing records efficiently.
- **Reporting and Visualization**: Real-time reports and interactive dashboards for key metrics.
- **AI-Powered Insights**: Integration with OpenAI GPT-4 to generate quick, actionable insights.
- **Comprehensive Logging**: Detailed logging for database operations and pipeline execution.
- **AWS Services Simulation**: Local tools configured to emulate AWS services for cost-effective development.

## Architecture and AWS Parallels

The pipeline architecture is designed to reflect AWS cloud services using local tools, showcasing cloud engineering principles in a cost-effective environment.

| Project Component                     | Simulated AWS Service                             |
| ------------------------------------- | ------------------------------------------------- |
| **`bucket/` Directory**               | Amazon S3 (Simple Storage Service)                |
| **SQLite Database (`sales.db`)**      | Amazon RDS (Relational Database Service)          |
| **Luigi Orchestration**               | AWS Step Functions                                |
| **Data Transformation Tasks**         | AWS Glue / AWS Data Pipeline                      |
| **FastAPI Endpoints**                 | AWS API Gateway & AWS Lambda Functions            |
| **Web Dashboard**                     | Amazon QuickSight                                 |
| **OpenAI GPT-4 Integration**          | Amazon SageMaker                                  |
| **Logging Mechanism**                 | Amazon CloudWatch                                 |

## Unformatted Input Data Simulation

To test the robustness and reliability of the data pipeline, we intentionally created multiple CSV files with different formats, delimiters, and inconsistencies. This approach simulates real-world scenarios where data may come from various sources with varying levels of quality and formatting.

### Splitting and Denormalizing the Original CSV

- **Multiple CSV Files**: The original sales data was split into several CSV files, each representing data from different days.
- **Varying Delimiters**: Files use different delimiters (`;` and `,`) to simulate inconsistent data sources.
- **Inconsistent Headers**: Some files have additional empty columns or mismatched headers.
- **Data Anomalies**: Included files with missing values, extra whitespace, and incorrect data types.
- **Filename Standardization**: Filenames follow a pattern `sales_data_YYYY-MM-DD.csv` to simulate daily data uploads.

By introducing these challenges, we ensured that the pipeline is capable of:

- **Normalizing Diverse Data**: Handling various CSV formats and correcting inconsistencies.
- **Validating and Cleaning Data**: Identifying and rectifying data anomalies before loading.
- **Ensuring Data Integrity**: Preventing corrupt or invalid data from entering the database.
- **Testing Upsert Logic**: Updating existing records and inserting new ones without duplication.

This deliberate complexity demonstrates the pipeline's resilience and the attention to detail in handling real-world data engineering challenges.

## Data Pipeline Flow

1. **Data Ingestion**: Raw CSV files are placed in the `bucket/` directory, simulating uploads to an S3 bucket.
2. **Data Extraction**: Luigi tasks extract data from CSV files, handling various delimiters and data inconsistencies.
3. **Data Transformation**: Data is cleaned, normalized, and validated to ensure high quality and consistency.
4. **Data Loading**: Transformed data is loaded into a SQLite database with upsert operations to manage new and existing records.
5. **Cleanup**: Temporary data and marker files are cleaned up to maintain an organized workspace.
6. **Reporting**: SQL queries generate reports, and data is visualized on an interactive web dashboard.
7. **AI-Powered Insights**: OpenAI GPT-4 provides quick insights based on the latest sales data and trends.

## Installation and Setup

### Prerequisites

- **Python**: Version 3.8 or higher
- **pip**: Python package installer

### Installation Steps

1. **Clone the Repository**

   ```bash
   git clone https://github.com/LucaCGN/sales_pipeline_aws.git
   cd sales_pipeline_aws
   ```

2. **Create a Virtual Environment (Recommended)**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Required Packages**

   ```bash
   pip install -r requirements.txt
   ```

4. **Set Up Environment Variables**

   - **OpenAI API Key**: Obtain your OpenAI API key and set it as an environment variable.

     ```bash
     export OPENAI_API_KEY='your-openai-api-key'  # On Windows: set OPENAI_API_KEY='your-openai-api-key'
     ```

     > **Important**: Do not hard-code your API key in the code for security reasons.

5. **Initialize the Database and Run the Pipeline**

   The application will automatically initialize the database and run the data pipeline on startup.

## Usage

### Running the Application

Start the FastAPI application using Uvicorn:

```bash
python -m uvicorn main:app --reload
```

### Accessing the Dashboard

Open your web browser and navigate to:

```
http://127.0.0.1:8000/dashboard/
```

### Uploading Data

To upload new sales data:

1. **Manual Upload**: Place your CSV files in the `bucket/` directory, following the naming convention `sales_data_YYYY-MM-DD.csv`.

   - Example: `sales_data_2024-10-18.csv`

2. **Data Format**: The pipeline is designed to handle various CSV formats, but the files should ideally contain the following columns:

   - `order_id`, `customer_id`, `product_id`, `quantity`, `price`, `order_date`

3. **Automated Pipeline Execution**: The pipeline will automatically detect new files and process them upon application startup or when triggered manually.

## Reports and Analytics

The dashboard provides real-time visualizations and reports:

- **Top 5 Customers by Revenue**
- **Top 5 Customers by Quantity Purchased**
- **Top 5 Best-Selling Products**
- **Top 5 Products by Revenue**
- **Monthly Revenue Trends**

### Generating Quick Insights

Click the **Quick Insights** button on the dashboard to generate AI-powered insights using OpenAI GPT-4. This feature provides actionable recommendations and highlights key business trends, enhancing decision-making capabilities.

## Technical Decisions and Considerations

- **Data Validation and Cleaning**: The pipeline robustly handles different CSV formats, missing columns, and malformed data to ensure data integrity.
- **Upsert Logic**: Implemented in the data loading phase to efficiently handle updates to existing records without data duplication.
- **Dashboard Design**: Developed an intuitive and informative dashboard using modern web technologies for effective data visualization.
- **AI Integration**: Leveraging OpenAI GPT-4 for generating insights adds significant value to data analysis and offers a competitive edge.
- **Modularity and Scalability**: The use of Luigi for task orchestration allows for easy addition of new tasks and scaling of the pipeline.
- **Attention to Detail**: Considered scenarios where existing orders need to be updated and ensured the pipeline accommodates such cases seamlessly.

## Logging and Monitoring

Logs are stored in the `logs/` directory:

- **`database_operations.log`**: Logs all database-related operations and queries.
- **`pipeline_execution.log`**: Logs each step of the data pipeline, including task dependencies and execution times.
- **`app.log`**: Captures general application events and API endpoint access logs.

These logs are crucial for auditing, troubleshooting, and ensuring transparency in the pipeline's operations.

## Error Handling and Data Validation

- **CSV Normalization**: Handles various delimiters (`;`, `,`) and inconsistent formatting across different CSV files.
- **Data Type Enforcement**: Ensures correct data types for each column, converting and correcting as necessary.
- **Missing Data Handling**: Fills missing columns with default values and removes rows with critical missing information.
- **Error Logging**: Any errors encountered during processing are logged with detailed messages for easy identification and resolution.
- **Resiliency**: The pipeline is designed to continue processing in the event of non-critical errors, ensuring maximum uptime and data availability.

## Automation and Scalability

- **Scheduled Execution**: The pipeline can be scheduled to run at specific intervals (e.g., daily) using cron jobs or task schedulers.
- **Modular Tasks**: Each Luigi task represents a discrete piece of functionality, allowing for parallel execution and easy scalability.
- **Resource Management**: Efficient use of resources ensures the pipeline can handle increasing volumes of data without significant performance degradation.

## Conclusion

This project showcases a comprehensive data engineering solution capable of handling real-world data challenges. By emulating AWS services and integrating advanced AI capabilities, it provides a robust, scalable, and insightful platform for e-commerce sales analysis.

The pipeline not only meets the immediate needs of data ingestion and reporting but also demonstrates a high level of attention to detail and seniority in project development. It effectively handles unformatted and inconsistent data, updates existing records without duplication, and offers valuable business insights through a well-designed dashboard and AI integration.

