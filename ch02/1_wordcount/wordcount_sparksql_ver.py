from pyspark.sql import SparkSession
import pyspark.sql.functions as f


if __name__ == '__main__':
    ss: SparkSession = (SparkSession.builder
                        .master("local")
                        .appName("wordCount SparkSQL ver")
                        .getOrCreate())

    # load data
    df = ss.read.text("data/words.txt")

    # df.show(truncate=False)

    # transformations
    df = (df.withColumn('word', f.explode(f.split(f.col('value'), ' ')))
          .withColumn("count", f.lit(1))).groupby("word").sum()

    df.orderBy("word").show()