"""The same silver logic in PySpark, for when the bronze layer outgrows one machine.
A parity test checks that both engines produce identical current-state tables."""

from __future__ import annotations

from pathlib import Path


def spark_session():
    from pyspark.sql import SparkSession

    return (SparkSession.builder.master("local[2]").appName("warranty-silver")
            .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate())


def silver_claims(spark, bronze: Path):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    ev = spark.read.parquet(str(bronze))
    ev = (ev.withColumn("_n", F.row_number().over(Window.partitionBy("table", "lsn").orderBy("ingested_ms")))
          .filter("_n = 1").drop("_n"))  # drop at-least-once duplicates
    latest = (ev.filter(F.col("table") == "claims")
              .withColumn("_r", F.row_number().over(Window.partitionBy("pk").orderBy(F.col("lsn").desc())))
              .filter("_r = 1").filter(F.col("op") != "d"))  # last write by LSN wins; deletes remove the row
    schema = ("claim_id STRING, vin STRING, dealer_id STRING, component STRING, status STRING, amount DOUBLE, "
              "claim_date STRING, labor_hours DOUBLE")
    return (latest.select(F.from_json("payload", schema).alias("p"), "lsn")
            .select("p.*", F.col("lsn").alias("_lsn"))
            .withColumn("claim_date", F.to_date("claim_date")))
