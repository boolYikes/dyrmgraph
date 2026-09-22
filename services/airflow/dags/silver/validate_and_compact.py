# TODO: implement this!s
# Runs hourly, Writes compacted parquets, incrementally (add and update rows if there were previous versions)
# Aggregates small files in the un-versioned silver bucket for the target date
# hence, it will compact at least 4 parquet files and if previous version exists for the day, update the existing one
# It will bump version too
# 1. Validates contiguity

# 2. Compaction

# 3. Version bump in pg

# example path s3://bucket/stage/compact/table/partition_date=xxxx/part-xxxx.parquet
