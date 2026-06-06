from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import os

spark = SparkSession.builder.appName("TopVerifiedUsers").getOrCreate()

posts = spark.read.option("header", True).csv("input/posts.csv")
users = spark.read.option("header", True).csv("input/users.csv")

posts = posts.withColumn("Likes", col("Likes").cast("int"))
posts = posts.withColumn("Retweets", col("Retweets").cast("int"))

verified = users.filter(col("Verified") == "True")

result = verified.join(posts, "UserID") \
    .withColumn("Engagement", col("Likes") + col("Retweets")) \
    .groupBy("UserID", "Username") \
    .agg(spark_sum("Engagement").alias("TotalEngagement")) \
    .orderBy(col("TotalEngagement").desc())

os.makedirs("outputs", exist_ok=True)

result.toPandas().to_csv(
    "outputs/top_verified_users.csv",
    index=False
)

spark.stop()