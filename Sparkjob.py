from pyspark.sql import SparkSession
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("Timberland_Analysis") \
      .getOrCreate()
csv_data = spark.read.option("header" , True).csv("/root/airflow/inputfiles/timberland_stock.csv")
csv_data.createOrReplaceTempView("mytable")
Peak_High_Price_Date = spark.sql("select Date from mytable where High = (select max(High) from mytable)")
Mean_Of_Close_Column = spark.sql("select avg(Close) as mean_of_column from mytable")
Max_of_Volume_Column = spark.sql("select max(Volume) as max_of_volume from mytable")
Min_of_Volume_Column = spark.sql("select min(Volume) as min_of_volume from mytable")
No_Of_days = spark.sql("SELECT COUNT(*) AS count_lower_than_60 FROM mytable WHERE Close < 60")
percentage = spark.sql("SELECT (COUNT(CASE WHEN High > 80 THEN 1 END) / COUNT(*)) * 100 AS percentage_high_above_80 FROM mytable")
Pearson_Correlation  = spark.sql("SELECT corr(High, Volume) AS correlation FROM mytable")
Max_High_Year = spark.sql("""SELECT YEAR(Date) AS Year, MAX(CAST(High AS DOUBLE)) AS max_high FROM mytable GROUP BY Year ORDER BY Year""")
Avg_Close_For_Each_Month = spark.sql("""SELECT YEAR(Date) AS Year, MONTH(Date) AS Month, AVG(Close) AS AvgClose FROM mytable GROUP BY Year, Month ORDER BY Year, Month""")
