from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


RAW_PATH = "data/raw/events.csv"


def run_spark_pipeline(path: str = RAW_PATH) -> None:
    spark = SparkSession.builder.appName("InsightFlowSpark").getOrCreate()

    df = spark.read.csv(path, header=True, inferSchema=True)

    daily = (
        df.withColumn("day", to_date(col("event_time")))
        .groupBy("day")
        .count()
        .orderBy("day")
    )

    by_event_type = df.groupBy("event_type").count().orderBy(col("count").desc())

    print("=== Daily events ===")
    daily.show(truncate=False)

    print("=== Event type distribution ===")
    by_event_type.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    run_spark_pipeline()
