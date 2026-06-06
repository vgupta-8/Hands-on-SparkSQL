from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

spark = SparkSession.builder.appName("SentimentEngagement").getOrCreate()

posts = spark.read.option("header", True).csv("input/posts.csv")

posts = posts.withColumn("Likes", col("Likes").cast("int"))
posts = posts.withColumn("Retweets", col("Retweets").cast("int"))
posts = posts.withColumn("SentimentScore", col("SentimentScore").cast("double"))

result = posts.withColumn(
    "Engagement",
    col("Likes") + col("Retweets")
).select(
    "PostID",
    "SentimentScore",
    "Engagement"
)

os.makedirs("outputs", exist_ok=True)

result.toPandas().to_csv(
    "outputs/sentiment_engagement.csv",
    index=False
)

spark.stop()