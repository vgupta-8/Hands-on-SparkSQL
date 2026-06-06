from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

# Initialize Spark Session
spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data
posts_df = spark.read.option("header", True).csv("input/posts.csv")

# TODO: Split the Hashtags column into individual hashtags and count the frequency of each hashtag and sort descending

hashtag_counts = posts_df.select(
    explode(split(col("hashtags"), " ")).alias("hashtag")
).groupBy("hashtag").count().orderBy(col("count").desc())

# Save result
import os
os.makedirs("outputs", exist_ok=True)
hashtag_counts.toPandas().to_csv("outputs/hashtag_trends.csv", index=False)
