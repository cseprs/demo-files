# process_lms_data.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

print("🔹 Starting LMS data processing...")

# Sample LMS data
data = [
    ("Alice", "Python", 85),
    ("Bob", "Power BI", 90),
    ("Charlie", "Fabric", 78),
    ("Alice", "Fabric", 88),
    ("Bob", "Python", 95)
]
columns = ["Student", "Course", "Score"]

# Create Spark DataFrame
df = spark.createDataFrame(data, columns)

print("Input data:")
df.show()

# Calculate average score per student
avg_scores = df.groupBy("Student").agg(avg("Score").alias("AvgScore"))

print("Average Scores per Student:")
avg_scores.show()

# Example business logic — filter top performers
top_students = avg_scores.filter(col("AvgScore") > 85)

print("Top performing students:")
top_students.show()

# Optionally, write results to Lakehouse
# top_students.write.mode("overwrite").parquet("Files/Output/top_students")

print("LMS data processing completed.")
