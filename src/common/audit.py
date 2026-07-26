from pyspark.sql import Row
from datetime import datetime
from pyspark.sql import SparkSession


def create_pipeline_audit(
    run_id,
    pipeline_name,
    source_file,
    target_table,
    total_records,
    good_records,
    bad_records,
    status,
    created_by,
    error_message=""
):

    return Row(
        run_id=run_id,
        pipeline_name=pipeline_name,
        source_file=source_file,
        target_table=target_table,
        start_time=datetime.now(),
        end_time=datetime.now(),
        total_records=total_records,
        good_records=good_records,
        bad_records=bad_records,
        status=status,
        error_message=error_message,
        #created_by="databricks",
        created_by=created_by,
        created_timestamp=datetime.now()
    )

def save_pipeline_audit(audit_record):

    spark = SparkSession.builder.getOrCreate()

    audit_df = spark.createDataFrame([audit_record])

    (
        audit_df.write
                .mode("append")
                .insertInto("banking.bronze.audit_pipeline_execution")
    )