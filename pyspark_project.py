from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Path to the MySQL Connector/J JAR file
jar_path = "/Users/anurag/Desktop/pyspark_project/mysql-connector-j-8.4.0/mysql-connector-j-8.4.0.jar"

# Initialize Spark session with the JAR file and Spark master
spark = SparkSession.builder \
    .appName("PySpark Project") \
    .config("spark.jars", jar_path) \
    .getOrCreate()

# Read the CSV file
df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)

# Show the CSV DataFrame
df_csv.show()

# MySQL connection properties
jdbc_url = "jdbc:mysql://localhost:3306/pyspark_db"
connection_properties = {
    "user": "root",
    "password": "rootpasswd",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read the MySQL table
df_mysql = spark.read.jdbc(url=jdbc_url, table="people", properties=connection_properties)

# Show the MySQL DataFrame
df_mysql.show()

# Combine the DataFrames
combined_df = df_csv.unionByName(df_mysql)

# Show the combined DataFrame
combined_df.show()

# Filter the DataFrame
filtered_df = combined_df.filter(combined_df.age > 30)

# Show the filtered DataFrame
filtered_df.show()

# Calculate the average age
avg_age_df = filtered_df.agg(avg("age").alias("average_age"))

# Show the average age
avg_age_df.show()

# Save the filtered DataFrame to a new CSV file with overwrite mode
filtered_df.write.mode('overwrite').csv("filtered_data.csv", header=True)

# Select specific columns
selected_df = filtered_df.select("name", "age")
selected_df.show()

# Sort the DataFrame by age in descending order
sorted_df = selected_df.orderBy("age", ascending=False)
sorted_df.show()

# Save the sorted DataFrame to a new CSV file with overwrite mode
sorted_df.write.mode('overwrite').csv("sorted_data.csv", header=True)

# Stop the Spark session
spark.stop()