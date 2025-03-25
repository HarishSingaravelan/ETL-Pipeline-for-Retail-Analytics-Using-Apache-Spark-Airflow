from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import (
    col,
    to_json,
    explode,
    count,
    round,
    format_number,
    when,
    to_date,
    size,
)
from pyspark.sql.functions import sum
from pyspark.sql.window import Window
from typing import Dict, Tuple
import traceback


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.mongodb.write.uri", "mongodb://localhost:27017")
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017")
        .getOrCreate()
    )


def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []

    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return date_list


def print_header():
    print("*" * 80)
    print("                        CANDY STORE DATA PROCESSING SYSTEM")
    print("                               Analysis Pipeline")
    print("*" * 80)


def print_processing_period(date_range: list):
    print("\n" + "=" * 80)
    print("PROCESSING PERIOD")
    print("-" * 80)
    print(f"Start Date: {date_range[0]}")
    print(f"End Date:   {date_range[-1]}")
    print("=" * 80)


def setup_configuration() -> Tuple[Dict, list]:
    """Setup application configuration"""
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


def load_config() -> Dict:
    """Load configuration from environment variables"""
    return {
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


def initialize_data_processor(spark: SparkSession, config: Dict) -> DataProcessor:
    """Initialize and configure the DataProcessor"""
    print("\nINITIALIZING DATA SOURCES")
    print("-" * 80)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    return data_processor


def print_processing_complete(total_cancelled_items: int) -> None:
    """Print processing completion message"""
    print("\nPROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total Cancelled Items: {total_cancelled_items}")


def print_daily_summary(orders_df, order_items_df, cancelled_count):
    """Print summary of daily processing"""
    processed_items = order_items_df.filter(col("quantity") > 0).count()
    print("\nDAILY PROCESSING SUMMARY")
    print("-" * 40)
    print(f"• Successfully Processed Orders: {orders_df.count()}")
    print(f"• Successfully Processed Items: {processed_items}")
    print(f"• Items Cancelled (Inventory): {cancelled_count}")
    print("-" * 40)


def generate_forecasts(
    data_processor: DataProcessor, final_daily_summary, output_path: str
):
    """Generate and save sales forecasts"""
    print("\nGENERATING FORECASTS")
    print("-" * 80)

    try:
        if final_daily_summary is not None and final_daily_summary.count() > 0:
            print("Schema before forecasting:", final_daily_summary.printSchema())
            forecast_df = data_processor.forecast_sales_and_profits(final_daily_summary)
            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, output_path, "sales_profit_forecast.csv"
                )
        else:
            print("Warning: No daily summary data available for forecasting")
    except Exception as e:
        print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")
        print("Stack trace:", traceback.format_exc())


def main():
    print_header()

    # Setup
    config, date_range = setup_configuration()
    print_processing_period(date_range)

    # Initialize processor
    spark = create_spark_session()
    data_processor = DataProcessor(spark)

    try:
        # Configure and load data
        data_processor.configure(config)

        print("Start batch processing for project 2!")

        # Generate forecasts
        try:
            # daily_summary_df follows the same schema as the daily_summary that you save to csv
            # schema:
            # - date: date - The business date
            # - num_orders: integer - Total number of orders for the day
            # - total_sales: decimal(10,2) - Total sales amount for the day
            # - total_profit: decimal(10,2) - Total profit for the day

            # Load data from .csv files to DataFrame
            customer_csv_df = data_processor.load_csv_data(config["customers_file"])
            product_csv_df = data_processor.load_csv_data(config["products_file"])

            print("\nCustomers DataFrame preview")
            customer_csv_df.show(5)
            print(
                f"Dimentions({customer_csv_df.count()}, {len(customer_csv_df.columns)})"
            )

            print("\nProducts DataFrame preview")
            product_csv_df.show(5)
            print(
                f"Dimentions({product_csv_df.count()}, {len(product_csv_df.columns)})"
            )

            # ------------------------------ Load data from  files to DataBases ------------------------------
            # Connection properties for writing to MySQL
            # connection_properties = {
            # "user": config["mysql_user"],
            # "password": config["mysql_password"],
            # "driver": "com.mysql.cj.jdbc.Driver"
            # }
            # # Write DataFrames to MySQL
            # customer_csv_df.write.jdbc(url=config["mysql_url"], table=config["customers_table"], mode="overwrite", properties=connection_properties)
            # product_csv_df.write.jdbc(url=config["mysql_url"], table=config["products_table"], mode="overwrite", properties=connection_properties)

            # # Get the date range for file retrieval
            date_range = get_date_range(
                config["mongo_start_date"], config["mongo_end_date"]
            )

            # # Generate a list of .json files to load
            # jsonFiles = [config["mongodb_collection_prefix"]+f for f in date_range]
            # print(jsonFiles)

            # # Load data from .json files to MongoDB
            # for jsonFile in jsonFiles:
            #     df=spark.read.option("multiline", "true").json(config["input_path"] +"/"+ jsonFile+".json")
            #     print(f"Data preview from {jsonFile}.json")
            #     df.show(5)
            #     print(f"Dimentions({df.count()}, {len(df.columns)})")
            #     df.write \
            #     .format("mongo") \
            #     .mode("overwrite") \
            #     .option("database", os.getenv("MONGO_DB")) \
            #     .option("collection", jsonFile) \
            #     .save()

            # df.show()
            # ----------------------------------------Creating of orders table ----------------------------------------------------
            # Load data from MongoDB
            data_processor.load_multiple_mongo(date_range)

            # Gets the required columns from the DataFrame and drops Qty
            data_processor.process_orders()

            # Load data from MySQL
            data_processor.load_mysql_data(config["products_table"])

            data_processor.joinTables()

            # Calculate the final quantity and total sales price
            data_processor.stock_calculation()

            # data_processor.save_to_csv(data_processor.updated, config["output_path"], "updated.csv")

            # Show final order table
            data_processor.itemPerOder()

            print("Preview of orders")
            data_processor.orders_df.show()

            # write to csv
            data_processor.save_to_csv(
                data_processor.orders_df, config["output_path"], "orders.csv"
            )

            # ----------------------------------------Creating of orders_line_items table ----------------------------------------------------

            data_processor.createOrderLine()

            print("Preview of order line items")
            data_processor.order_line_items_df.show()

            data_processor.save_to_csv(
                data_processor.order_line_items_df,
                config["output_path"],
                "order_line_items.csv",
            )
            # -------------------------------------------products updated---------------------------------------------------------------------------------

            data_processor.createProducts()

            print("Preview of products updated")
            data_processor.products_updated_df.show()

            data_processor.save_to_csv(
                data_processor.products_updated_df,
                config["output_path"],
                "products_updated.csv",
            )
            # -------------------------------------------Daily Summary---------------------------------------------------------------------------------

            data_processor.daily_summary()

            print("Preview of products updated")
            data_processor.daily_summary_df.show()

            data_processor.save_to_csv(
                data_processor.daily_summary_df,
                config["output_path"],
                "daily_summary.csv",
            )

            data_processor.daily_summary_df = data_processor.daily_summary_df
            forecast_df = data_processor.forecast_sales_and_profits(
                data_processor.daily_summary_df
            )
            # round the forecast_df

            forecast_df = forecast_df.select(
                col("date"),
                round(col("forecasted_sales"), 2).alias("forecasted_sales"),
                round(col("forecasted_profit"), 2).alias("forecasted_profit"),
            )
            print("Preivew of forecast_df")
            forecast_df.show()

            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, config["output_path"], "sales_profit_forecast.csv"
                )
        except Exception as e:
            print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise
    finally:
        print("\nCleaning up...")
        spark.stop()


if __name__ == "__main__":
    main()
