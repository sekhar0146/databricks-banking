from pyspark.sql.functions import col


def validate_customer_data(df):

    good_df = df.filter(
        col("customer_id").isNotNull() &
        col("customer_name").isNotNull() &
        col("balance").isNotNull() &
        (col("balance") >= 0)
    )

    bad_df = df.subtract(good_df)

    return good_df, bad_df