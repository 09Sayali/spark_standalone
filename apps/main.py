from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format

def create_spark():
  spark = SparkSession.builder\
    .appName("spark_annalect")\
    .config("spark.jars")\
    .getOrCreate()
  
  return spark



def main():

  spark = create_spark()

  file = "/opt/spark-data/data.csv"
  raw_df = spark.read.load(file, 
                          format = "csv",
                          inferSchema="true",
                          sep=",",
                          header="true")
  print(raw_df.printSchema())
  print(f'num records: {raw_df.count()}')
  print(raw_df.show(10, False))

  #create temp table, so that it is easy to build sql query against it
  raw_df.createOrReplaceTempView("raw_tbl")

  # 3. What was the most exported grade for each year and origin?
  print('Question 3')
  df3 = spark.sql('''
                  
        SELECT year, originName, gradeName
        FROM ( 
              SELECT year, originName, gradeName, total_export_quantity,
                      RANK() OVER(PARTITION BY year, originName
                                  ORDER BY total_export_quantity DESC) AS rnk
              FROM ( 
                    SELECT year, originName, 
                           gradeName,
                           sum(quantity) AS total_export_quantity 
                    FROM raw_tbl
                    GROUP by year, originName, gradeName
                  ) AS temp
            ) AS tbl
        WHERE rnk = 1  
        ''')
  print(df3.show(50, False))

  # 2. For UK, which destinations have a total quantity greater than 100,000?
  print('Question 2')
  df2 = spark.sql('''
                SELECT originName, destinationName
                FROM raw_tbl
                where originName = 'United Kingdom'
                GROUP BY originName, destinationName
                HAVING SUM(quantity) > 100000
            ''')
  print(df2.show(50, False))

  # 1. What are the top 5 destinations for oil produced in Albania?
  print('Question 1')
  df1 = spark.sql('''
                SELECT originName, destinationName, sum(quantity) as total_quantity
                FROM raw_tbl
                where originName = 'Albania'
                GROUP by originName, destinationName
                ORDER BY total_quantity DESC
                LIMIT 5
            ''')
  print(df1.show(10, False))

  ## another way to solve this question would be to use window function to dense_rank() to make the query scalable. 
  df0 = spark.sql('''
                      SELECT destinationName 
                      FROM (
                            SELECT  destinationName,
                                    dense_rank() over(ORDER BY total_quantity) as rnk
                            FROM (
                                  SELECT originName, destinationName, sum(quantity) as total_quantity
                                  FROM raw_tbl
                                  where originName = 'Albania'
                                  GROUP by originName, destinationName 
                                ) AS t1
                          ) as t2
                      WHERE rnk <= 5
                      ''')
  print(df0.show(10, False))


  
if __name__ == '__main__':
  main()