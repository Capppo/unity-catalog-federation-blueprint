# Databricks notebook source
# MAGIC %md
# MAGIC # Reconciliation – CTU `ctu_financial` (original) vs copy
# MAGIC
# MAGIC For every table in the source schema:
# MAGIC 1. **Columns**: same columns on both sides (case-insensitive)?
# MAGIC 2. **Row counts**: same number of rows?
# MAGIC 3. **Content**: every row hashed after type normalisation; the two *multisets* of hashes are compared,
# MAGIC    so duplicates and missing/extra/changed rows are all detected.
# MAGIC
# MAGIC Normalisation avoids false mismatches between MariaDB and PostgreSQL: numbers are compared as
# MAGIC `decimal(38,6)`, dates/timestamps as `yyyy-MM-dd HH:mm:ss`, strings trimmed, NULL as a marker.

# COMMAND ----------

SRC = "jdbc_ctulab_dev.ctu_financial"                 # original: CTU MariaDB via JDBC views
DST = "fed_supabaselab_dev.ctu_financial"        # copy: Supabase  <- adjust catalog/schema
SAMPLE_DIFFS = 5                                      # mismatching rows to show per table

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (ByteType, ShortType, IntegerType, LongType, FloatType,
                               DoubleType, DecimalType, DateType, TimestampType,
                               TimestampNTZType, BooleanType)

NUMERIC = (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)
TEMPORAL = (DateType, TimestampType, TimestampNTZType)


def normalise(df):
    """Lower-case column names and turn every value into a comparable string."""
    out = []
    for f in df.schema.fields:
        c = F.col(f"`{f.name}`")
        if isinstance(f.dataType, NUMERIC):
            v = c.cast("decimal(38,6)").cast("string")
        elif isinstance(f.dataType, TEMPORAL):
            v = F.date_format(c.cast("timestamp"), "yyyy-MM-dd HH:mm:ss")
        elif isinstance(f.dataType, BooleanType):
            v = c.cast("int").cast("string")
        else:
            v = F.trim(c.cast("string"))
        out.append(F.coalesce(v, F.lit("<NULL>")).alias(f.name.lower()))
    return df.select(out)


def with_hash(df, cols):
    return df.withColumn("_h", F.sha2(F.concat_ws("|", *[F.col(c) for c in cols]), 256))


tables = sorted(r.tableName for r in spark.sql(f"SHOW TABLES IN {SRC}").collect())
dst_tables = {r.tableName.lower() for r in spark.sql(f"SHOW TABLES IN {DST}").collect()}

report, samples = [], {}
for t in tables:
    if t.lower() not in dst_tables:
        report.append((t, None, None, None, None, "MISSING IN COPY", ""))
        continue

    a = normalise(spark.table(f"{SRC}.`{t}`"))
    b = normalise(spark.table(f"{DST}.`{t.lower()}`"))

    only_a_cols = sorted(set(a.columns) - set(b.columns))
    only_b_cols = sorted(set(b.columns) - set(a.columns))
    cols = sorted(set(a.columns) & set(b.columns))
    col_note = ""
    if only_a_cols or only_b_cols:
        col_note = f"cols only in source {only_a_cols}, only in copy {only_b_cols}"

    a, b = with_hash(a.select(cols), cols).cache(), with_hash(b.select(cols), cols).cache()
    n_a, n_b = a.count(), b.count()

    # Multiset comparison: counts per hash on each side
    ca = a.groupBy("_h").agg(F.count("*").alias("na"))
    cb = b.groupBy("_h").agg(F.count("*").alias("nb"))
    d = (ca.join(cb, "_h", "full")
           .fillna(0, ["na", "nb"])
           .where("na <> nb"))
    only_a = d.select(F.sum(F.greatest(F.col("na") - F.col("nb"), F.lit(0)))).first()[0] or 0
    only_b = d.select(F.sum(F.greatest(F.col("nb") - F.col("na"), F.lit(0)))).first()[0] or 0

    ok = n_a == n_b and only_a == 0 and only_b == 0 and not col_note
    report.append((t, n_a, n_b, int(only_a), int(only_b), "OK" if ok else "DIFF", col_note))

    if only_a or only_b:
        bad = d.select("_h").limit(SAMPLE_DIFFS)
        samples[t] = (a.join(bad, "_h").withColumn("side", F.lit("source"))
                       .unionByName(b.join(bad, "_h").withColumn("side", F.lit("copy")))
                       .drop("_h"))
    a.unpersist(); b.unpersist()

display(spark.createDataFrame(
    report,
    "table string, rows_source long, rows_copy long, only_in_source long, only_in_copy long, "
    "status string, note string"))

# COMMAND ----------

# Sample mismatching rows (if any), one table at a time
for t, df in samples.items():
    print(f"=== {t}: sample of rows that differ ===")
    display(df)
if not samples:
    print("No row-level differences.")