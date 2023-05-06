
def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.



if __name__ == '__main__':
    print_hi('PyCharm')

import pyspark
 #   import findspark
    from pyspark import SparkContext

    from pyspark import SparkConf
    from pyspark.sql import SparkSession, Window

    import pyspark.sql.types as t
    import pyspark.sql.functions as f
    spark_session = (SparkSession.builder
                        .master("local")
                        .appName("task app")
                        .config(conf=SparkConf())
                        .getOrCreate())
#Setup Stage

#The First Attempt

    data = [("Maryna", 25), ("Nina", 45)]
    schema = t.StructType([
          t.StructField("name",t.StringType(),True),
          t.StructField("age", t.IntegerType(),True)])
    df = spark_session.createDataFrame(data,schema)
    df.show()

# Schema

    df.printSchema() #non action: do not requare to run Pipeline

# The Second Attempt
# Read.csv()
   # path = "https://drive.google.com/file/d/1ZNoOLowZSxpaosAy8n92munQhZjxtmnP/view?usp=share_link"
    path =
    from_csv_header_df = spark_session.read.csv(path, header = True, nullValue='null')
    from_csv_header_df.show()


    from_csv_header_df.explain(mode="extended")

#BigData Diploma Project Task Use PySpark!

# Performed by Maryna Novozhylova
# marina.novozhilova@kname.edu.ua

#Processing of IMDb Datasets

# Extraction Stage

# 1. Create appropriate schemas for all 7 datasets.

# Schema for name.basics.tsv.gz

    from_tsv_df_name_schema = t.StructType([t.StructField("nconst", t.StringType(), True),
                                            t.StructField("primaryName", t.StringType(), True),
                                            t.StructField("birthYear", t.IntegerType(), True),
                                            t.StructField("deathYear", t.IntegerType(), True),
                                            t.StructField("primaryProfession", t.StringType(), True),
                                            t.StructField("knownForTitles", t.StringType(), True)])

# Schema for title_akas.tsv.gz

    from_tsv_df_title_akas_schema = t.StructType([t.StructField("titleId", t.StringType(), True),
                                                 t.StructField("ordering", t.IntegerType(), True),
                                                 t.StructField("title", t.StringType(), True),
                                                 t.StructField("region", t.StringType(), True),
                                                 t.StructField("language", t.StringType(), True),
                                                 t.StructField("types", t.StringType(), True),
                                                 t.StructField("attributes", t.StringType(), True),
                                                 t.StructField("isOriginalTitle", t.BooleanType(), True)])

# Schema for title.basics.tsv.gz

    from_title_basics_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                            t.StructField("titleType", t.StringType(), True),
                                            t.StructField("primaryTitle", t.StringType(), True),
                                            t.StructField("originalTitle", t.StringType(), True),
                                            t.StructField("isAdult", t.StringType(), True),
                                            t.StructField("startYear", t.IntegerType(), True),
                                            t.StructField("endYear", t.IntegerType(), True),
                                            t.StructField("runtimeMinutes", t.IntegerType(), True),
                                            t.StructField("genres", t.StringType(), True)])

# Schema for title.crew.tsv.gz

    from_title_crew_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                           t.StructField("directors", t.StringType(), True),
                                           t.StructField("writers", t.StringType(), True)])

# Schema for title.principals.tsv.gz

    from_title_principals_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                                t.StructField("ordering", t.IntegerType(), True),
                                                t.StructField("nconst", t.StringType(), True),
                                                t.StructField("category", t.StringType(), True),
                                                t.StructField("job", t.StringType(), True),
                                                t.StructField("characters", t.StringType(), True)])

# Schema for title.episode.tsv.gz

    title_episode_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                         t.StructField("parentTconst", t.StringType(), True),
                                         t.StructField("seasonNumber", t.IntegerType(), True),
                                         t.StructField("episodeNumber", t.IntegerType(), True)])

# Schema for title.ratings.tsv.gz

    title_ratings_schema = t.StructType([t.StructField("tconst", t.StringType(),True),
                                         t.StructField("averageRating", t.FloatType(),True),
                                         t.StructField("numVotes", t.IntegerType(),True)])


# 2. Depending on the schemas above create corresponding DataFrames by reading data from the PC.

# Read name.basics.tsv.gz
import pyspark
 #   import findspark
    from pyspark import SparkContext

    from pyspark import SparkConf
    from pyspark.sql import SparkSession, Window

    import pyspark.sql.types as t
    import pyspark.sql.functions as f
    spark_session = (SparkSession.builder
                        .master("local")
                        .appName("task app")
                        .config(conf=SparkConf())
                        .getOrCreate())
#Setup Stage

#The First Attempt

    data = [("Maryna", 25), ("Nina", 45)]
    schema = t.StructType([
          t.StructField("name",t.StringType(),True),
          t.StructField("age", t.IntegerType(),True)])
    df = spark_session.createDataFrame(data,schema)
    df.show()

# Schema

    df.printSchema() #non action: do not requare to run Pipeline

# The Second Attempt
# Read.csv()
   # path = "https://drive.google.com/file/d/1ZNoOLowZSxpaosAy8n92munQhZjxtmnP/view?usp=share_link"
    path =
    from_csv_header_df = spark_session.read.csv(path, header = True, nullValue='null')
    from_csv_header_df.show()


    from_csv_header_df.explain(mode="extended")

#BigData Diploma Project Task Use PySpark!

# Performed by Maryna Novozhylova
# marina.novozhilova@kname.edu.ua

#Processing of IMDb Datasets

# Extraction Stage

# 1. Create appropriate schemas for all 7 datasets.

# Schema for name.basics.tsv.gz

    from_tsv_df_name_schema = t.StructType([t.StructField("nconst", t.StringType(), True),
                                            t.StructField("primaryName", t.StringType(), True),
                                            t.StructField("birthYear", t.IntegerType(), True),
                                            t.StructField("deathYear", t.IntegerType(), True),
                                            t.StructField("primaryProfession", t.StringType(), True),
                                            t.StructField("knownForTitles", t.StringType(), True)])

# Schema for title_akas.tsv.gz

    from_tsv_df_title_akas_schema = t.StructType([t.StructField("titleId", t.StringType(), True),
                                                 t.StructField("ordering", t.IntegerType(), True),
                                                 t.StructField("title", t.StringType(), True),
                                                 t.StructField("region", t.StringType(), True),
                                                 t.StructField("language", t.StringType(), True),
                                                 t.StructField("types", t.StringType(), True),
                                                 t.StructField("attributes", t.StringType(), True),
                                                 t.StructField("isOriginalTitle", t.BooleanType(), True)])

# Schema for title.basics.tsv.gz

    from_title_basics_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                            t.StructField("titleType", t.StringType(), True),
                                            t.StructField("primaryTitle", t.StringType(), True),
                                            t.StructField("originalTitle", t.StringType(), True),
                                            t.StructField("isAdult", t.StringType(), True),
                                            t.StructField("startYear", t.IntegerType(), True),
                                            t.StructField("endYear", t.IntegerType(), True),
                                            t.StructField("runtimeMinutes", t.IntegerType(), True),
                                            t.StructField("genres", t.StringType(), True)])

# Schema for title.crew.tsv.gz

    from_title_crew_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                           t.StructField("directors", t.StringType(), True),
                                           t.StructField("writers", t.StringType(), True)])

# Schema for title.principals.tsv.gz

    from_title_principals_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                                t.StructField("ordering", t.IntegerType(), True),
                                                t.StructField("nconst", t.StringType(), True),
                                                t.StructField("category", t.StringType(), True),
                                                t.StructField("job", t.StringType(), True),
                                                t.StructField("characters", t.StringType(), True)])

# Schema for title.episode.tsv.gz

    title_episode_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                         t.StructField("parentTconst", t.StringType(), True),
                                         t.StructField("seasonNumber", t.IntegerType(), True),
                                         t.StructField("episodeNumber", t.IntegerType(), True)])

# Schema for title.ratings.tsv.gz

    title_ratings_schema = t.StructType([t.StructField("tconst", t.StringType(),True),
                                         t.StructField("averageRating", t.FloatType(),True),
                                         t.StructField("numVotes", t.IntegerType(),True)])


# 2. Depending on the schemas above create corresponding DataFrames by reading data from the PC.

# Read name.basics.tsv.gz

    path_name = r"C:\Users\Maryna_N\PycharmProjects\pythonProject10\InitialData\title.basics.tsv.gz"
    from_tsv_df_name = spark_session.read.csv(path_name,
                                              sep=r"\t",
                                              nullValue='\\N',
                                              header=True,
                                              schema=from_tsv_df_name_schema)

# 3. Check if everything went right with any method on DataFrames.

    from_tsv_df_name.show(truncate=False)
    count = from_tsv_df_name.count()
    print("Number of tsv_df_name Rows is ", count)

#    from_tsv_df_title_basics.show(truncate=False)
#    count=from_tsv_df_title_basics.count()
#    print("Number of tsv_df_basics Rows is ", count)












# See PyCharm help at https://www.jetbrains.com/help/pycharm/
