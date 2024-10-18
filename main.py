# main.py

from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import os
import shutil
import subprocess
from datetime import datetime
from scripts.config import config  # Absolute import from the scripts package

app = FastAPI()
templates = Jinja2Templates(directory="templates")

@app.on_event("startup")
async def startup_event():
    """
    Event handler that runs on application startup.

    - Initializes the database.
    - Runs the data pipeline.

    AWS Parallel:
    - Emulates initialization tasks that might occur in an AWS Lambda function or during EC2 instance startup.
    - Runs the data pipeline, similar to triggering an AWS Step Functions workflow.
    """
    config.app_logger.info("Startup event triggered.")

    # Initialize the database
    from scripts.initialize_db import initialize_database
    try:
        initialize_database()
        config.database_logger.info("Database initialized successfully.")
        config.app_logger.info("Database initialized successfully.")
    except Exception as e:
        config.database_logger.error(f"Database initialization failed: {e}")
        config.app_logger.error(f"Database initialization failed: {e}")

    # Run the pipeline
    try:
        # Use absolute path to luigi_pipeline.py
        pipeline_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts', 'luigi_pipeline.py')
        subprocess.run(["python", pipeline_script], check=True)
        config.pipeline_logger.info("Data pipeline executed successfully on startup.")
        config.app_logger.info("Data pipeline executed successfully on startup.")
    except subprocess.CalledProcessError as e:
        config.pipeline_logger.error(f"Pipeline execution failed during startup: {e}")
        config.app_logger.error(f"Pipeline execution failed during startup: {e}")

@app.post("/upload/")
async def upload_csv(file: UploadFile = File(...)):
    """
    Endpoint to upload CSV files.

    Steps:
    - Saves the uploaded file to the 'bucket' directory, simulating an S3 upload.
    - Names the file with the current date to simulate daily data uploads.

    AWS Parallel:
    - Emulates uploading files to an AWS S3 bucket via a web interface or API.
    """
    config.app_logger.info("Received file upload request.")
    try:
        # Create a filename with current date to simulate daily uploads
        today = datetime.now().strftime('%Y-%m-%d')
        filename = f'sales_data_{today}.csv'
        file_location = os.path.join(config.BUCKET_PATH, filename)

        with open(file_location, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        config.database_logger.info(f"File '{filename}' uploaded successfully.")
        config.app_logger.info(f"File '{filename}' uploaded successfully.")
        return {"info": f"File '{filename}' uploaded successfully"}
    except Exception as e:
        config.database_logger.error(f"Failed to upload file: {e}")
        config.app_logger.error(f"Failed to upload file: {e}")
        return {"error": "File upload failed."}

@app.get("/run-pipeline/")
def run_pipeline():
    """
    Endpoint to trigger the data pipeline manually.

    AWS Parallel:
    - Simulates manually triggering an AWS Step Functions workflow or an AWS Glue job via an API call.
    """
    config.app_logger.info("Received request to run data pipeline.")
    try:
        # Use absolute path to luigi_pipeline.py
        pipeline_script = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts', 'luigi_pipeline.py')
        subprocess.run(["python", pipeline_script], check=True)
        config.pipeline_logger.info("Data pipeline executed successfully via API.")
        config.app_logger.info("Data pipeline executed successfully via API.")
        return {"info": "Data pipeline executed successfully"}
    except subprocess.CalledProcessError as e:
        config.pipeline_logger.error(f"Pipeline execution failed: {e}")
        config.app_logger.error(f"Pipeline execution failed: {e}")
        return {"error": f"Pipeline execution failed: {e}"}

@app.get("/dashboard/")
def dashboard(request: Request):
    """
    Endpoint to display the sales dashboard.

    AWS Parallel:
    - Serves a web dashboard, similar to AWS QuickSight dashboards for data visualization.
    """
    config.app_logger.info("Dashboard accessed.")
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/api/top-customers/")
def api_top_customers():
    """
    API endpoint to fetch top 5 customers by revenue.

    AWS Parallel:
    - Provides data via an API, similar to exposing data from AWS API Gateway connected to AWS Lambda functions querying RDS.
    """
    from scripts.reports import get_top_customers
    config.app_logger.info("Fetching top customers by revenue.")
    try:
        df = get_top_customers()
        data = df.to_dict(orient='records')
        config.pipeline_logger.info("Fetched top customers by revenue.")
        config.app_logger.info("Fetched top customers by revenue.")
        return JSONResponse(content=data)
    except Exception as e:
        config.pipeline_logger.error(f"Failed to fetch top customers: {e}")
        config.app_logger.error(f"Failed to fetch top customers: {e}")
        return JSONResponse(content={"error": "Failed to fetch top customers."}, status_code=500)

@app.get("/api/top-products/")
def api_top_products():
    """
    API endpoint to fetch top 5 best-selling products by quantity.

    AWS Parallel:
    - Provides data via an API, similar to exposing data from AWS API Gateway connected to AWS Lambda functions querying RDS.
    """
    from scripts.reports import get_top_products
    config.app_logger.info("Fetching top products by quantity.")
    try:
        df = get_top_products()
        data = df.to_dict(orient='records')
        config.pipeline_logger.info("Fetched top products by quantity.")
        config.app_logger.info("Fetched top products by quantity.")
        return JSONResponse(content=data)
    except Exception as e:
        config.pipeline_logger.error(f"Failed to fetch top products: {e}")
        config.app_logger.error(f"Failed to fetch top products: {e}")
        return JSONResponse(content={"error": "Failed to fetch top products."}, status_code=500)

@app.get("/api/monthly-revenue/")
def api_monthly_revenue():
    """
    API endpoint to fetch monthly revenue data.

    AWS Parallel:
    - Provides data via an API, similar to exposing data from AWS API Gateway connected to AWS Lambda functions querying RDS.
    """
    from scripts.reports import get_monthly_revenue
    config.app_logger.info("Fetching monthly revenue data.")
    try:
        df = get_monthly_revenue()
        data = df.to_dict(orient='records')
        config.pipeline_logger.info("Fetched monthly revenue data.")
        config.app_logger.info("Fetched monthly revenue data.")
        return JSONResponse(content=data)
    except Exception as e:
        config.pipeline_logger.error(f"Failed to fetch monthly revenue: {e}")
        config.app_logger.error(f"Failed to fetch monthly revenue: {e}")
        return JSONResponse(content={"error": "Failed to fetch monthly revenue."}, status_code=500)

@app.get("/api/top-products-by-revenue/")
def api_top_products_by_revenue():
    """
    API endpoint to fetch top 5 products by revenue.

    AWS Parallel:
    - Provides data via an API, similar to exposing data from AWS API Gateway connected to AWS Lambda functions querying RDS.
    """
    from scripts.reports import get_top_products_by_revenue
    config.app_logger.info("Fetching top products by revenue.")
    try:
        df = get_top_products_by_revenue()
        data = df.to_dict(orient='records')
        config.pipeline_logger.info("Fetched top products by revenue.")
        config.app_logger.info("Fetched top products by revenue.")
        return JSONResponse(content=data)
    except Exception as e:
        config.pipeline_logger.error(f"Failed to fetch top products by revenue: {e}")
        config.app_logger.error(f"Failed to fetch top products by revenue: {e}")
        return JSONResponse(content={"error": "Failed to fetch top products by revenue."}, status_code=500)

@app.get("/api/top-customers-by-quantity/")
def api_top_customers_by_quantity():
    """
    API endpoint to fetch top 5 customers by total quantity purchased.

    AWS Parallel:
    - Provides data via an API, similar to exposing data from AWS API Gateway connected to AWS Lambda functions querying RDS.
    """
    from scripts.reports import get_top_customers_by_quantity
    config.app_logger.info("Fetching top customers by quantity.")
    try:
        df = get_top_customers_by_quantity()
        data = df.to_dict(orient='records')
        config.pipeline_logger.info("Fetched top customers by quantity.")
        config.app_logger.info("Fetched top customers by quantity.")
        return JSONResponse(content=data)
    except Exception as e:
        config.pipeline_logger.error(f"Failed to fetch top customers by quantity: {e}")
        config.app_logger.error(f"Failed to fetch top customers by quantity: {e}")
        return JSONResponse(content={"error": "Failed to fetch top customers by quantity."}, status_code=500)

@app.get("/api/quick-insights/")
def api_quick_insights():
    """
    API endpoint to fetch quick insights generated by an LLM.

    AWS Parallel:
    - Demonstrates the integration of AI services, similar to using AWS SageMaker for generating insights.
    - Provides advanced analytics capabilities on top of the data.

    Note:
    - Uses OpenAI's GPT-4 model to generate insights based on the reports.
    """
    from scripts.reports import get_all_reports_as_text
    config.app_logger.info("Fetching quick insights.")

    # Prepare messages for the LLM
    reports_text = get_all_reports_as_text()
    system_message = """
        You are an expert data analyst. Based on the following sales data reports, provide key insights and observations that could help improve the business.

        Present assertive insights with short and objective key takeaways.

        Use standard markdown for formatting, incorporating emoticons where appropriate.

        Organize your response into sections such as **Attention 🚨**, **Growth Opportunities 🌱**, etc.

        Ensure consistent formatting in your messages for clear readability.

        **Formatting Guidelines:**

        - Use `**bold**` for emphasis on key data points.
        - Use `*italics*` for subtle highlights or emphasis.
        - Organize insights into clear sections with relevant emoticons:
        - **Attention 🚨**
        - **Top Performers 🏆**
        - **Best-Selling Products 📈**
        - **Growth Opportunities 🌱**
        - **Underperforming Areas ⚠️**
        - Keep bullet points concise and objective for quick readability.
        """

    user_message = reports_text

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message}
    ]

    try:
        response = config.client.chat.completions.create(
            model="gpt-4o",  # Use the appropriate model
            messages=messages,
            max_tokens=1000,
            temperature=0.1,  # Adjust temperature as needed
        )

        # Accessing the content correctly based on the response structure
        summary = response.choices[0].message.content.strip()
        config.app_logger.info("Quick insights fetched successfully.")
        return {"insights": summary}
    except Exception as e:
        config.pipeline_logger.error(f"Failed to fetch quick insights: {e}")
        config.app_logger.error(f"Failed to fetch quick insights: {e}")
        return {"error": "Failed to fetch quick insights."}

@app.get("/")
def index():
    """
    Redirects the root URL to the dashboard.

    AWS Parallel:
    - Similar to setting up route mappings in AWS API Gateway or AWS Elastic Beanstalk.
    """
    config.app_logger.info("Redirecting root URL to dashboard.")
    return RedirectResponse(url="/dashboard/")
