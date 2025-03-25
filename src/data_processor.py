from pyspark.sql import SparkSession, DataFrame
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
import os
import glob
import shutil
import decimal
import numpy as np
from time_series import ProphetForecaster
from datetime import datetime, timedelta
from pyspark.sql.types import DoubleType, DecimalType


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Initialize all class properties
        self.config = None
        self.current_inventory = None
        self.inventory_initialized = False
        self.original_products_df = None  # Store original products data
        self.reload_inventory_daily = False  # New flag for inventory reload
        self.order_items = None
        self.products_updated_df = None
        self.products_df = None
        self.customers_df = None
        self.transactions_df = None
        self.orders_df = None
        self.order_line_items_df = None
        self.daily_summary_df = None
        self.orders_df = None
        self.joined = None
        self.total_cancelled_items = 0
        self.mongo_uri = os.getenv("MONGODB_URI")
        self.mongo_db = os.getenv("MONGO_DB")
        self.mongo_collection_prefix = os.getenv("MONGO_COLLECTION_PREFIX")
        self.jdbc_url = os.getenv("MYSQL_URL")
        self.db_user = os.getenv("MYSQL_USER")
        self.db_password = os.getenv("MYSQL_PASSWORD")

    def configure(self, config: Dict) -> None:
        """Configure the data processor with environment settings"""
        self.config = config
        self.reload_inventory_daily = config.get("reload_inventory_daily", False)
        print("\nINITIALIZING DATA SOURCES")
        print("-" * 80)
        if self.reload_inventory_daily:
            print("Daily inventory reload: ENABLED")
        else:
            print("Daily inventory reload: DISABLED")

    def finalize_processing(self) -> None:
        """Finalize processing and create summary"""
        print("\nPROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total Cancelled Items: {self.total_cancelled_items}")

    # ------------------------------------------------------------------------------------------------
    # Try not to change the logic of the time series forecasting model
    # DO NOT change functions with prefix _
    # ------------------------------------------------------------------------------------------------
    def forecast_sales_and_profits(
        self, daily_summary_df: DataFrame, forecast_days: int = 1
    ) -> DataFrame:
        """
        Main forecasting function that coordinates the forecasting process
        """
        try:
            # Build model
            model_data = self.build_time_series_model(daily_summary_df)

            # Calculate accuracy metrics
            metrics = self.calculate_forecast_metrics(model_data)

            # Generate forecasts
            forecast_df = self.make_forecasts(model_data, forecast_days)

            return forecast_df

        except Exception as e:
            print(
                f"Error in forecast_sales_and_profits: {str(e)}, please check the data"
            )
            return None

    def print_inventory_levels(self) -> None:
        """Print current inventory levels for all products"""
        print("\nCURRENT INVENTORY LEVELS")
        print("-" * 40)

        inventory_data = self.current_inventory.orderBy("product_id").collect()
        for row in inventory_data:
            print(
                f"• {row['product_name']:<30} (ID: {row['product_id']:>3}): {row['current_stock']:>4} units"
            )
        print("-" * 40)

    def build_time_series_model(self, daily_summary_df: DataFrame) -> dict:
        """Build Prophet models for sales and profits"""
        print("\n" + "=" * 80)
        print("TIME SERIES MODEL CONSTRUCTION")
        print("-" * 80)

        model_data = self._prepare_time_series_data(daily_summary_df)
        return self._fit_forecasting_models(model_data)

    def calculate_forecast_metrics(self, model_data: dict) -> dict:
        """Calculate forecast accuracy metrics for both models"""
        print("\nCalculating forecast accuracy metrics...")

        # Get metrics from each model
        sales_metrics = model_data["sales_model"].get_metrics()
        profit_metrics = model_data["profit_model"].get_metrics()

        metrics = {
            "sales_mae": sales_metrics["mae"],
            "sales_mse": sales_metrics["mse"],
            "profit_mae": profit_metrics["mae"],
            "profit_mse": profit_metrics["mse"],
        }

        # Print metrics and model types
        print("\nForecast Error Metrics:")
        print(f"Sales Model Type: {sales_metrics['model_type']}")
        print(f"Sales MAE: ${metrics['sales_mae']:.2f}")
        print(f"Sales MSE: ${metrics['sales_mse']:.2f}")
        print(f"Profit Model Type: {profit_metrics['model_type']}")
        print(f"Profit MAE: ${metrics['profit_mae']:.2f}")
        print(f"Profit MSE: ${metrics['profit_mse']:.2f}")

        return metrics

    def make_forecasts(self, model_data: dict, forecast_days: int = 7) -> DataFrame:
        """Generate forecasts using Prophet models"""
        print(f"\nGenerating {forecast_days}-day forecast...")

        forecasts = self._generate_model_forecasts(model_data, forecast_days)
        forecast_dates = self._generate_forecast_dates(
            model_data["training_data"]["dates"][-1], forecast_days
        )

        return self._create_forecast_dataframe(forecast_dates, forecasts)

    def _prepare_time_series_data(self, daily_summary_df: DataFrame) -> dict:
        """Prepare data for time series modeling"""
        data = (
            daily_summary_df.select("date", "total_sales", "total_profit")
            .orderBy("date")
            .collect()
        )

        dates = np.array([row["date"] for row in data])
        sales_series = np.array([float(row["total_sales"]) for row in data])
        profit_series = np.array([float(row["total_profit"]) for row in data])

        self._print_dataset_info(dates, sales_series, profit_series)

        return {"dates": dates, "sales": sales_series, "profits": profit_series}

    def _print_dataset_info(
        self, dates: np.ndarray, sales: np.ndarray, profits: np.ndarray
    ) -> None:
        """Print time series dataset information"""
        print("Dataset Information:")
        print(f"• Time Period:          {dates[0]} to {dates[-1]}")
        print(f"• Number of Data Points: {len(dates)}")
        print(f"• Average Daily Sales:   ${np.mean(sales):.2f}")
        print(f"• Average Daily Profit:  ${np.mean(profits):.2f}")

    def _fit_forecasting_models(self, data: dict) -> dict:
        """Fit Prophet models to the prepared data"""
        print("\nFitting Models...")
        sales_forecaster = ProphetForecaster()
        profit_forecaster = ProphetForecaster()

        sales_forecaster.fit(data["sales"])
        profit_forecaster.fit(data["profits"])
        print("Model fitting completed successfully")
        print("=" * 80)

        return {
            "sales_model": sales_forecaster,
            "profit_model": profit_forecaster,
            "training_data": data,
        }

    def _generate_model_forecasts(self, model_data: dict, forecast_days: int) -> dict:
        """Generate forecasts from both models"""
        return {
            "sales": model_data["sales_model"].predict(forecast_days),
            "profits": model_data["profit_model"].predict(forecast_days),
        }

    def _generate_forecast_dates(self, last_date: datetime, forecast_days: int) -> list:
        """Generate dates for the forecast period"""
        return [last_date + timedelta(days=i + 1) for i in range(forecast_days)]

    def _create_forecast_dataframe(self, dates: list, forecasts: dict) -> DataFrame:
        """Create Spark DataFrame from forecast data"""
        forecast_rows = [
            (date, float(sales), float(profits))
            for date, sales, profits in zip(
                dates, forecasts["sales"], forecasts["profits"]
            )
        ]

        return self.spark.createDataFrame(
            forecast_rows, ["date", "forecasted_sales", "forecasted_profit"]
        )

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def load_csv_data(self, data_path: str) -> DataFrame:
        """Load data from a given path"""
        return self.spark.read.csv(data_path, header=True, inferSchema=True)

    # def load_jsonto_mongo(self, data_path: str) -> DataFrame:
    #     """Load data from a given path"""
    #     for jsonFile in jsonFiles:
    #             df=self.spark.read.option("multiline", "true").json(config["input_path"] +"/"+ jsonFile+".json")
    #             print(f"Data preview from {jsonFile}.json")
    #             df.show(5)
    #             print(f"Dimentions({df.count()}, {len(df.columns)})")
    #             df.write \
    #             .format("mongo") \
    #             .mode("overwrite") \
    #             .option("database", os.getenv("MONGO_DB")) \
    #             .option("collection", jsonFile) \
    #             .save()

    def load_mongo_data(self, date: str) -> DataFrame:
        """Load data from MongoDB"""
        return (
            self.spark.read.format("mongo")
            .option("uri", os.getenv("MONGODB_URI"))
            .option("database", self.mongo_db)
            .option("collection", self.mongo_collection_prefix + date)
            .load()
        )

    def load_mysql_data(self, table: str):
        """Load data from MySQL database"""
        df = (
            self.spark.read.format("jdbc")
            .option("url", self.jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table)
            .option("user", self.db_user)
            .option("password", self.db_password)
            .load()
        )

        self.products_df = df

    def joinTables(self):
        """Join orders and products DataFrames"""
        self.joined = self.orders_df.join(
            self.products_df, on="product_id", how="left"
        ).orderBy("order_datetime")

    def load_multiple_mongo(self, date_range: list):
        """Load data from multiple MongoDB collections"""
        df_total = None
        for date in date_range:
            df = self.load_mongo_data(date)
            df_total = df if df_total is None else df_total.union(df)

        transactions_exploded_df = df_total.withColumn("item", explode(col("items")))

        self.transactions_df = transactions_exploded_df

    def process_orders(self):
        """Load orders DataFrame"""
        df = self.transactions_df.select(
            col("transaction_id").alias("order_id"),
            col("timestamp").alias("order_datetime"),
            col("customer_id"),
            col("item.product_id"),
            col("item.qty"),
        ).filter(col("qty").isNotNull())
        self.orders_df = df

    def multiply_QtyNSales(self, df: DataFrame) -> DataFrame:
        """Multiply quantity by sales"""
        return (
            df.groupBy("order_id", "order_datetime", "customer_id")
            .agg(
                format_number(sum(col("qty") * col("sales_price")), 2).alias(
                    "total_amount"
                ),
                count("product_id").alias("num_items"),
            )
            .orderBy("order_id")
        )

    def stock_calculation(self) -> DataFrame:
        """Calculate stock levels dynamically to each order"""
        # Define window for cumulative stock updates

        window_spec = (
            Window.partitionBy("product_id")
            .orderBy("order_datetime")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        # Compute cumulative quantity sold
        updated_df = self.joined.withColumn(
            "cumulative_qty", sum("qty").over(window_spec)
        ).withColumn("remaining_stock", col("stock") - col("cumulative_qty"))

        # Ensure stock never goes negative
        updated_df = updated_df.withColumn(
            "final_qty",
            when(col("remaining_stock") >= 0, col("qty"))  # Normal case
            .when(
                col("stock") - (col("cumulative_qty") - col("qty")) > 0,
                col("stock") - (col("cumulative_qty") - col("qty")),
            )  # Partial fulfillment
            .otherwise(0),  # No stock left
        ).withColumn(
            "updated_stock",
            when(col("remaining_stock") >= 0, col("remaining_stock")).otherwise(
                0
            ),  # Stock stops at 0
        )  # .drop("cumulative_qty", "remaining_stock")

        self.updated_df = updated_df.orderBy("product_id", "order_datetime")

    def itemPerOder(self) -> DataFrame:
        """Calculate the number of items per order"""
        df = (
            self.updated_df.groupBy("order_id", "order_datetime", "customer_id")
            .agg(
                format_number(sum(col("final_qty") * col("sales_price")), 2).alias(
                    "total_amount"
                ),
                count("product_id").alias("num_items"),
            )
            .orderBy("order_id")
        )

        self.orders_df = df

    def createOrderLine(self) -> DataFrame:
        """Create order line items DataFrame"""
        self.order_line_items_df = self.updated_df.select(
            col("order_id"),
            col("product_id"),
            col("final_qty").alias("quantity"),
            format_number(col("sales_price"), 2).alias("unit_price"),
            format_number(col("final_qty") * col("sales_price"), 2).alias("line_total"),
        ).orderBy("order_id", "product_id")

    def createProducts(self) -> DataFrame:
        """Create products DataFrame"""
        self.products_updated_df = (
            self.updated_df.groupBy("product_id", "product_name", "stock")
            .agg((col("stock") - sum("final_qty")).alias("current_stock"))
            .drop("stock")
        )

    def daily_summary(self) -> DataFrame:
        """Create daily summary DataFrame"""
        self.daily_summary_df = (
            self.updated_df.withColumn("date", to_date(col("order_datetime")))
            .groupBy("date")
            .agg(
                count(col("order_id")).alias("num_orders"),
                round(sum(col("final_qty") * col("sales_price")), 2).alias(
                    "total_sales"
                ),
                round(
                    sum(col("final_qty") * (col("sales_price") - col("cost_to_make"))),
                    2,
                ).alias("total_profit"),
            )
            .orderBy("date")
        )
