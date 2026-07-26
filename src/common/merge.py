from delta.tables import DeltaTable


def merge_customer(
    spark,
    source_df,
    target_table
):

    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("t")
        .merge(
            source_df.alias("s"),
            "t.customer_id = s.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )