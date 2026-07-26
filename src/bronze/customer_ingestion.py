from src.common import logger
from src.common.config import load_config
from src.common.logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import *


def main():
    spark = SparkSession.builder.getOrCreate()

    config = load_config()

    logger = get_logger(__name__)

    catalog = config["catalog"]
    bronze_schema = config["schemas"]["bronze"]
    volume_path = config["volumes"]["landing"]
    customer_table = config["tables"]["customer"]

    logger.info(f"Catalog       : {catalog}")
    logger.info(f"Bronze Schema : {bronze_schema}")
    logger.info(f"Volume        : {volume_path}")
    logger.info(f"Table         : {customer_table}")

    logger.info("Customer ingestion started")

    # Read the customer data from the landing zone/ databricks volume
    
    customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("balance", DecimalType(10, 2), True)
    ])

    file_name = config["files"]["customer"]

    file_path = f"{volume_path}/{file_name}"

    logger.info(f"Reading file : {file_path}")

    df = (
        spark.read
        .option("header", "true")
        .schema(customer_schema)
        .csv(file_path)
    )
    df.printSchema()
    df.show(5)

    logger.info(f"Source record count : {df.count()}")

    # Write the data into the bronze schema as a Delta table
    (
        df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{catalog}.{bronze_schema}.{customer_table}")
    )

    logger.info(f"Loaded data into {catalog}.{bronze_schema}.{customer_table}")


if __name__ == "__main__":
    main()