from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os


def import_dependencies():
    global DataProcessor, SparkSession, load_dotenv
    from data_processor import DataProcessor
    from pyspark.sql import SparkSession
    from dotenv import load_dotenv


# Default arguments for DAG
default_args = {
    "owner": "airflow",  # Who owns/maintains this DAG
    "depends_on_past": False,  # Tasks don't depend on past runs
    "start_date": datetime(2024, 9, 23),  # When the DAG should start running
    "email_on_failure": False,  # Don't send emails on task failure
    "email_on_retry": False,  # Don't send emails on task retries
    "retries": 0,  # Number of times to retry a failed task
    "retry_delay": timedelta(minutes=5),  # Wait 5 minutes between retries
}

# Initialize DAG
dag = DAG(
    "example_dag",  # Unique identifier for the DAG
    default_args=default_args,
    description="Batch Processing of data",
    schedule_interval=timedelta(days=1),  # Run the DAG once per day
    catchup=False,  # Don't run for historical dates when DAG is first enabled
    tags=["example"],  # Tags for organizing DAGs in the Airflow UI
)


def create_spark_session():
    """Creates and returns a Spark session when needed."""
    from pyspark.sql import (
        SparkSession,
    )  # Import inside function to reduce DAG load time

    return (
        SparkSession.builder.appName("OrderProcessingDAG")
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .getOrCreate()
    )


def load_config():
    """Loads configuration from environment variables and pushes to XCom."""
    import_dependencies()
    from dotenv import load_dotenv  # Import inside function to avoid global execution

    load_dotenv()
    config = {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "mongo_start_date": os.getenv("MONGO_START_DATE"),
        "mongo_end_date": os.getenv("MONGO_END_DATE"),
        "input_path": os.getenv("INPUT_PATH"),
        "customers_file": os.getenv("CUSTOMERS_FILE"),
        "products_file": os.getenv("PRODUCTS_FILE"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
    }

    return config


def import_data():
    """Imports data from MySQL and MongoDB, then prepares it for processing."""
    import_dependencies()
    spark = create_spark_session()
    from main import get_date_range

    data_processor = DataProcessor(spark)
    config = load_config()

    try:
        # 🔹 Load products and customers from MySQL
        date_range = get_date_range(
            config["mongo_start_date"], config["mongo_end_date"]
        )
        data_processor.load_multiple_mongo(date_range)

        data_processor.process_orders()

        # Load data from MySQL
        data_processor.load_mysql_data(config["products_table"])

        data_processor.joinTables()

        # Calculate the final quantity and total sales price
        data_processor.stock_calculation()

    except Exception as e:
        print(f" Error in importing data: {str(e)}")


def process_order_line_items():
    """Processes order line items by extracting product-level details."""
    import_dependencies()
    spark = create_spark_session()
    config = load_config()

    data_processor = DataProcessor(spark)

    try:
        data_processor.createOrderLine()
        data_processor.save_to_csv(
            data_processor.order_line_items_df,
            config["output_path"],
            "order_line_items.csv",
        )
    except Exception as e:
        print(f"Error in processing order line items: {str(e)}")


def process_orders():
    """Processes orders in batches with detailed logging."""
    import_dependencies()
    spark = create_spark_session()
    config = load_config()
    data_processor = DataProcessor(spark)

    try:
        data_processor.itemPerOder()
        data_processor.save_to_csv(
            data_processor.orders_df, config["output_path"], "orders.csv"
        )

    except Exception as e:
        print(f"Error in processing orders: {str(e)}")


def process_daily_summary():
    """Aggregates daily transactions and generates a summary of sales and profits."""
    import_dependencies()
    spark = create_spark_session()
    config = load_config()
    data_processor = DataProcessor(spark)

    try:
        data_processor.daily_summary()
        data_processor.save_to_csv(
            data_processor.daily_summary_df,
            config["output_path"],
            "daily_summary.csv",
        )
    except Exception as e:
        print(f"Error in processing daily summary: {str(e)}")


def generate_forecasts():
    """Generates sales and profit forecasts."""
    import_dependencies()
    spark = create_spark_session()

    config = load_config()

    data_processor = DataProcessor(spark)

    try:

        forecast_df = data_processor.forecast_sales_and_profits(
            data_processor.daily_summary_df
        )
        data_processor.save_to_csv(
            forecast_df, config["output_path"], "sales_profit_forecast.csv"
        )

    except Exception as e:
        print(f"Error in generating forecasts: {str(e)}")


# Define tasks
task_load_config = PythonOperator(
    task_id="load_config",
    python_callable=load_config,
    provide_context=True,
    dag=dag,
)

task_import_From_Mongo_n_MySQL = PythonOperator(
    task_id="import_From_Mongo_n_MySQL",
    python_callable=import_data,
    provide_context=True,
    dag=dag,
)

task_generate_orders = PythonOperator(
    task_id="generate_orders",
    python_callable=process_orders,
    provide_context=True,
    dag=dag,
)

task_generate_order_line_items = PythonOperator(
    task_id="generate_order_line_items",
    python_callable=process_order_line_items,
    provide_context=True,
    dag=dag,
)

task_generate_daily_summary = PythonOperator(
    task_id="generate_daily_summary",
    python_callable=process_daily_summary,
    provide_context=True,
    dag=dag,
)

task_generate_forecasts = PythonOperator(
    task_id="generate_forecasts",
    python_callable=generate_forecasts,
    provide_context=True,
    dag=dag,
)

# Task Dependencies
(
    task_load_config
    >> task_import_From_Mongo_n_MySQL
    >> task_generate_orders
    >> task_generate_order_line_items
    >> task_generate_daily_summary
    >> task_generate_forecasts
)
