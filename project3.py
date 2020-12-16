import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():
    spark = SparkSession\
        .builder\
        .appName("Project 3")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    sc = spark.sparkContext

    lines = sc.textFile('/docs/gutenberg_works_5g.txt')
    # lines = sc.textFile('/docs/gutenberg_works_50m.txt')
    lines = lines.toDF(StringType())
    lines = lines.toDF('Text')

    # PART 1 count the number of dash words
    words_df = lines.rdd.flatMap(lambda ffd: ffd.Text.split()).toDF(StringType())
    words_filter_df = words_df.filter(words_df.value.contains('-'))
    count_of_dash_words = words_filter_df.count()
    print("Count of words with a dash: ")
    print(count_of_dash_words)
    print()

    # PART 2 count the number of occurrences for the top 25 occurring words
    words_group = words_df.groupby("value")
    words_count_df = words_group.count()
    words_count_df = words_count_df.toDF('Word', 'Occurrences')
    words_count_df = words_count_df.orderBy(words_count_df.Occurrences.desc())
    words_count_take_25 = words_count_df.take(25)
    print("Occurrences of top 25 words: ")
    print("Word | Count")
    for row in words_count_take_25:
        print(row.Word + ": " + str(row.Occurrences))
    print()

    # PART 3 count the number of occurrences for the top 25 occurring lengths
    words_length_df = words_count_df.withColumn('Length', length(words_count_df.Word))
    words_length_group = words_length_df.groupby('Length')
    length_count_df = words_length_group.sum('Occurrences')
    length_count_df = length_count_df.toDF('Length', 'Occurrences')
    length_count_df = length_count_df.orderBy(length_count_df.Occurrences.desc())
    length_count_take_25 = length_count_df.take(25)
    print("Occurrences of top 25 lengths: ")
    print("Length | Count")
    for row in length_count_take_25:
        print(str(row.Length) + ": " + str(row.Occurrences))

    spark.stop()


if __name__ == "__main__":
    main()
