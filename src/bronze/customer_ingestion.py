from src.common import logger
from src.common.config import load_config
from src.common.logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.common.validation import validate_customer_data
from datetime import datetime
from src.common.audit import create_pipeline_audit, save_pipeline_audit

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

    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
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
    good_df, bad_df = validate_customer_data(df)
    logger.info(f"Good records : {good_df.count()}")
    logger.info(f"Bad records  : {bad_df.count()}")

    # Write the data into the bronze schema as a Delta table
    (
        good_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{catalog}.{bronze_schema}.{customer_table}")
    )

    logger.info(f"Loaded data into {catalog}.{bronze_schema}.{customer_table}")

    current_user = spark.sql("SELECT current_user()").first()[0]

    audit_record = create_pipeline_audit(
        run_id=run_id,
        pipeline_name="customer_ingestion",
        source_file=file_name,
        target_table=customer_table,
        total_records=df.count(),
        good_records=good_df.count(),
        bad_records=bad_df.count(),
        status="SUCCESS",
        created_by=current_user
    )

    save_pipeline_audit(audit_record)

    logger.info("Pipeline audit record inserted.")


if __name__ == "__main__":
    main()