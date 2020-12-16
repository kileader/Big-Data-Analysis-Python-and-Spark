import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():
    spark = SparkSession\
        .builder\
        .appName("PoemParser")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    data = [
        Row("<p>Nature's", "first", "green", "is", "gold,</p>", "<p>", "Her"),
        Row("hardest", "hue", "to", "hold.</p><p>", "Her", "early", "leaf's"),
        Row("a", "flower;</p><p>", "But", "only", "so", "an", "hour.</p><p>"),
        Row("Then", "leaf", "subsides", "to", "leaf.</p><p>", "So", "Eden"),
        Row("sank", "to", "grief,</p><p>", "So", "dawn", "goes", "down"),
        Row("to", "day.</p>", "<p>", "Nothing", "gold", "can", "stay.</p>")
        ]

    poem_dataframe = spark.createDataFrame(data)
    print("Poem in a Data Set:")
    poem_dataframe.show()

    named_columns_df = poem_dataframe.toDF('Column1', 'Column2', 'Column3', 'Column4', 'Column5', 'Column6', 'Column7') 
    print("Poem with Renamed Columns:")
    named_columns_df.show()

    col1_filter_df = named_columns_df.filter(named_columns_df.Column1.contains('to'))
    print("Filtered on Column 1:")
    col1_filter_df.show()

    col1_2_filter_df = named_columns_df.filter(named_columns_df.Column1.contains('to') | named_columns_df.Column2.contains('to'))
    print("Filtered on Columns 1 and 2:")
    col1_2_filter_df.show()

    cols_1_through_4_filter_df = named_columns_df.filter(named_columns_df.Column1.contains('to') | named_columns_df.Column2.contains('to') | named_columns_df.Column3.contains('to') | named_columns_df.Column4.contains('to'))
    print("Filtered on Columns 1 through 4:")
    cols_1_through_4_filter_df.show()

    concatenated_df = named_columns_df.withColumn('FreeFormText', concat_ws(' ', named_columns_df.Column1, named_columns_df.Column2, named_columns_df.Column3, named_columns_df.Column4, named_columns_df.Column5, named_columns_df.Column6, named_columns_df.Column7)) 
    print("New concatenated column:") 
    concatenated_df.show()

    free_form_df = concatenated_df.select(concatenated_df.FreeFormText)
    print("Free form text:")
    free_form_df.show()

    hello_df = free_form_df.withColumn("FreeFormText", lit('Hello World!'))
    print("Rows of Hello World!:")
    hello_df.show()

    split_df = free_form_df.withColumn("FreeFormText", split(free_form_df.FreeFormText, " "))
    print("Each individual word, nested:")
    split_df.show()

    new_column_df = free_form_df.withColumn("MyNewColumn", expr('split(FreeFormText, " ")'))
    print("Created a new column:")
    new_column_df.show()

    words_df = free_form_df.rdd.flatMap(lambda ffd: ffd.FreeFormText.split(' ')).toDF(StringType())
    print("Each individual word:")
    words_df.show()

    line_rows = free_form_df.collect()
    print("Nested, collected lines:")
    for line_row in line_rows:
        print(line_row.FreeFormText)

    words_df_take_15 = words_df.take(15)
    print("Nested, collected words:")
    for word in words_df_take_15:
        print(word.value)

    num_rows = words_df.count()
    print("Number words in poem:")
    print(num_rows)

    spark.stop()


if __name__ == "__main__":
    main()
