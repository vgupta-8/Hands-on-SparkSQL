from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
import os

spark = SparkSession.builder.appName("EngagementByAge").getOrCreate()

posts = spark.read.option("header", True).csv("input/posts.csv")
users = spark.read.option("header", True).csv("input/users.csv")

posts = posts.withColumn("Likes", col("Likes").cast("int"))
posts = posts.withColumn("Retweets", col("Retweets").cast("int"))

engagement = posts.withColumn(
    "Engagement", col("Likes") + col("Retweets")
)

result = engagement.join(users, "UserID") \
    .groupBy("AgeGroup") \
    .agg(avg("Engagement").alias("AvgEngagement")) \
    .orderBy(col("AvgEngagement").desc())

os.makedirs("outputs", exist_ok=True)

result.toPandas().to_csv(
    "outputs/engagement_by_age.csv",
    index=False
)

spark.stop()