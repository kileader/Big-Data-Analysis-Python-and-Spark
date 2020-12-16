from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():
    spark = SparkSession\
        .builder\
        .appName("TortoiseVHare")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    data = [
        Row("Galumph Cerone", "Tortoise", "M", 148.822, "Mai Saito", "Rabbit", "F", 144.057),
        Row("Rasheed Tyson", "Tortoise", "M", 169.587, "Skippy Jaworski", "Rabbit", "M", 703.148),
        Row("Curly Baldini", "Rabbit", "M", 41.656, "Galumph Cerone", "Tortoise", "M", 146.513),
        Row("Raffaella Rodriguez", "Rabbit", "F", 24.042, "Leather Tuscadero", "Tortoise", "F", 150.507),
        Row("Vaiva Stablum", "Tortoise", "F", 140.605, "Bun-Bun Ewart", "Rabbit", "M", 281.153),
        Row("Har-Har De Vries", "Rabbit", "M", 78.916, "Takako Yukimura", "Tortoise", "F", 138.150),
        Row("Tito Evangelista", "Tortoise", "M", 136.887, "Iria Breda", "Rabbit", "F", 227.689),
        Row("Curly Baldini", "Rabbit", "M", 54.662, "Nikolai Evangelista", "Tortoise", "M", 175.112),
        Row("Nneka Ngannou", "Rabbit", "F", 38.081, "Painter Shepherd", "Tortoise", "M", 140.605),
        Row("Bourne Runn", "Tortoise", "M", 145.817, "Curly Baldini", "Rabbit", "M", 1053.189)
        ]

    rabbit_tortoise_dataframe = spark.createDataFrame(data)
    print("Tortoise versus Hare Data:")
    rabbit_tortoise_dataframe.show()

    rabbit_tortoise_dataframe = rabbit_tortoise_dataframe.toDF('Winners_Name', 'Winners_Animal', 'Winners_Sex', 'Winners_Time', 'Losers_Name', 'Losers_Animal', 'Losers_Sex', 'Losers_Time') 
    winners_group = rabbit_tortoise_dataframe.groupby("Winners_Name")

    wins_by_name_df = winners_group.count()
    wins_by_name_df = wins_by_name_df.toDF('Winners_Name', 'Num_Wins')
    print("Winner count by name:")
    wins_by_name_df.show()

    wins_by_name_df = wins_by_name_df.orderBy(wins_by_name_df.Num_Wins.desc())
    print("Ordered by number of wins:")
    wins_by_name_df.show()

    wins_by_name_df = wins_by_name_df.orderBy(wins_by_name_df.Winners_Name.asc())
    print("Ordered by name:")
    wins_by_name_df.show()

    spark.stop()


if __name__ == "__main__":
    main()
