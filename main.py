#if __name__ == '__main__':

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window

import pyspark.sql.types as t
import pyspark.sql.functions as f

from pyspark.ml.feature import VectorAssembler
from pyspark.ml import evaluation

spark_session = (SparkSession.builder
                 .master("local")
                 .appName("task app")
                 .config(conf=SparkConf())
                 .getOrCreate())

# Setup Stage

# The First Step

data = [("Maryna", 25), ("Nina", 45)]
schema = t.StructType([
          t.StructField("name", t.StringType(), True),
          t.StructField("age", t.IntegerType(), True)])
df = spark_session.createDataFrame(data, schema)
df.show()

# Schema
df.printSchema()

# BigData Diploma Project Task Use PySpark!

# Performed by Maryna Novozhylova
# marina.novozhilova@kname.edu.ua

# Processing of IMDb Datasets

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

title_ratings_schema = t.StructType([t.StructField("tconst", t.StringType(), True),
                                     t.StructField("averageRating", t.FloatType(), True),
                                     t.StructField("numVotes", t.IntegerType(), True)])


# 2. Depending on the schemas above create corresponding DataFrames by reading data from the PC.

# Read name.basics.tsv.gz

# path_name = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\name.basics.tsv.gz"
path = "InitialData\\name.basics.tsv.gz"
from_tsv_df_name = spark_session.read.csv(path,
                                          sep=r"\t",
                                          nullValue='\\N',
                                          header=True,
                                          schema=from_tsv_df_name_schema)

# Read title_akas.tsv.gz

# path_tsv_df_title_akas = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\title.akas.tsv.gz"
path = "InitialData\\title.akas.tsv.gz"
from_tsv_df_title_akas = spark_session.read.csv(path,
                                                sep=r"\t",
                                                header=True,
                                                nullValue='\\N',
                                                schema=from_tsv_df_title_akas_schema)

# Read title_basics.tsv.gz

# path_title_basics = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\title.basics.tsv.gz"
path = "InitialData\\title.basics.tsv.gz"
from_tsv_df_title_basics = spark_session.read.csv(path,
                                                  sep=r'\t',
                                                  header=True,
                                                  nullValue='\\N',
                                                  schema=from_title_basics_schema)

# Read title.crew.tsv.gz

# path_title_crew = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\title.crew.tsv.gz"
path = "InitialData\\title.crew.tsv.gz"
from_tsv_df_title_crew = spark_session.read.csv(path,
                                                sep=r'\t',
                                                header=True,
                                                nullValue='\\N',
                                                schema=from_title_crew_schema)

# Read title.principals.tsv.gz

# path_title_principals = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\title.principals.tsv.gz"
path = "InitialData\\title.principals.tsv.gz"
from_tsv_df_title_principals = spark_session.read.csv(path,
                                                      sep=r'\t',
                                                      header=True,
                                                      nullValue='\\N',
                                                      schema=from_title_principals_schema)

# Read title.episode.tsv.gz

# path_title_episode = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\title.episode.tsv.gz"
path = "InitialData\\title.episode.tsv.gz"
title_episode = spark_session.read.csv(path,
                                       sep=r'\t',
                                       header=True,
                                       nullValue='\\N',
                                       schema=title_episode_schema)

# Read title.ratings.tsv.gz

# path_title_ratings = r"C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\InitialData\\title.ratings.tsv.gz"
path = "InitialData\\title.ratings.tsv.gz"
title_ratings = spark_session.read.csv(path,
                                       sep=r'\t',
                                       header=True,
                                       nullValue='\\N',
                                       schema=title_ratings_schema)

# 3. Check if everything went right with any method on DataFrames.

from_tsv_df_name.show(truncate=False)
count = from_tsv_df_name.count()
print("Number of tsv_df_name Rows is ", count)

from_tsv_df_title_basics.show(truncate=False)
count = from_tsv_df_title_basics.count()
print("Number of tsv_df_basics Rows is ", count)

from_tsv_df_name.show(truncate=False)
count = from_tsv_df_name.count()
print("Number of tsv_df_name Rows is ", count)

from_tsv_df_title_akas.show(truncate=False)
count = from_tsv_df_title_akas.count()
print("Number of tsv_df_akas Rows is ", count)

from_tsv_df_title_basics.show(truncate=False)
count = from_tsv_df_title_basics.count()
print("Number of tsv_df_basics Rows is ", count)

from_tsv_df_title_crew.show(truncate=False)
count = from_tsv_df_title_crew.count()
print("Number of tsv_df_crew Rows is ", count)

from_tsv_df_title_principals.show(truncate=False)
count = from_tsv_df_title_principals.count()
print("Number of tsv_df_principals Rows is ", count)

title_episode.show(truncate=False)
count = title_episode.count()
print("Number of tsv_df_episode Rows is ", count)

title_ratings.show(truncate=False)
count = title_ratings.count()
print("Number of tsv_df_ratings Rows is ", count)


# Transformation Stage

# 1. Get all titles of series/movies etc. that are available in Ukrainian.

# First of all I tried to examine this task using column "language" - nothing

df_language_ua = from_tsv_df_title_akas.filter((from_tsv_df_title_akas.language == "ua")
                                               | (from_tsv_df_title_akas.language == "ukr")
                                               | (from_tsv_df_title_akas.language == "UA"))
df_language_ua.show(truncate=False)
count = df_language_ua.count()
print("Number of Ukr titles of series/movies etc using column language", count)

# So to solve the task I applied selection according column "region"

UA_title = from_tsv_df_title_akas.filter(f.col("region") == "UA").select("title", "region")
UA_title.show(truncate=False)
count = UA_title.count()
print("Number of UA_title Rows is ", count)

# 2. Get  the list of people’s names, who were born in the 19 th century.

# Statistics on Column

from_tsv_df_name.select("birthYear").describe().show(truncate=False)

start_year = 1801
fin_year = 1900

df_name_19th_senture_short = from_tsv_df_name.filter((from_tsv_df_name.birthYear >= start_year)
                                                     & (from_tsv_df_name.birthYear <= fin_year))\
                                                     .select("primaryName", "birthYear").orderBy("birthYear")
df_name_19th_senture_short.show(truncate=False)

count = df_name_19th_senture_short.count()
print("Quantity of people, who were born in the 19 th century is ", count)

# 3. Get titles of all movies that last more than 2 hours.

movie_length_2_hours = 120
Movie_type = "movie"

df_movie_length_2_hours = from_tsv_df_title_basics.filter((f.col("runtimeMinutes") > movie_length_2_hours)
                                                          & (f.col("titleType") == Movie_type)).select("titleType",
                                                                                                       "primaryTitle",
                                                                                                       "originalTitle",
                                                                                                       "runtimeMinutes")

df_movie_length_2_hours = df_movie_length_2_hours.withColumn("lengthInHours",
                                                             df_movie_length_2_hours.runtimeMinutes/60)
df_movie_length_2_hours.count()
df_movie_length_2_hours.show(truncate=False)

# 4. Get names of people, corresponding movies/series and characters they played in those films.

nconst = "nconst"
category = "category"
job = "job"
characters = "characters"
primaryName = "primaryName"
tconst = "tconst"

df_join_principals_name = from_tsv_df_title_principals.join(from_tsv_df_name, on="nconst", how="left")
df_select_left = df_join_principals_name.select(primaryName, category, characters, tconst)

df_select_title = from_tsv_df_title_basics.select(from_tsv_df_title_basics.tconst,
                                                  from_tsv_df_title_basics.titleType,
                                                  from_tsv_df_title_basics.primaryTitle)

df_select_left_ = df_select_left.join(df_select_title, on="tconst", how="left").drop("tconst")

df_select_left_.show()
df_select_left_.count()

# 5. Get information about how many adult movies/series etc. there are per region.
#    Get the top 100 of them from the region with the biggest count to the region with the smallest one.

df_adult_movies = from_tsv_df_title_akas.join(from_tsv_df_title_basics,
                                              from_tsv_df_title_basics.tconst == from_tsv_df_title_akas.titleId,
                                              how="left")
df_adult_basics = df_adult_movies.filter((df_adult_movies.isAdult == "1")
                                         & (df_adult_movies.region != "null")).select(df_adult_movies.isAdult,
                                                                                      df_adult_movies.region)
# df_adult_basics.count()
df_adult_basics.groupby(f.col("region")).count().orderBy("count",ascending=False).show(20)
df_adult_region = df_adult_basics.groupby(f.col("region")).count().orderBy("count", ascending=False)
df_adult_region.show(100)

# 6. Get information about how many episodes in each TV Series. Get the top 50 of them starting from the TV Series with the biggest quantity of episodes

# df_episode_group = title_episode.filter(title_episode.parentTconst != "null")
df_episode_group = title_episode.filter(f.col("parentTconst").isNotNull())
df_episode_group_count_= df_episode_group.groupby("parentTconst").count().orderBy("count", ascending=False)
df_episode_group_titles = df_episode_group_count_.join(from_tsv_df_title_basics,
                                                       from_tsv_df_title_basics.tconst
                                                       == df_episode_group_count_.parentTconst,
                                                       how='left')

df_episode_group_titles = df_episode_group_titles.select("primaryTitle", "count").orderBy("count", ascending=False)
df_episode_group_titles.show()

# 7. Get 10 titles of the most popular movies/series etc. by each decade.

decade = 10   # New column divided by 10

df_basic = from_tsv_df_title_basics.select("tconst", "originalTitle", "startYear").filter(f.col("startYear").isNotNull())

ratings = title_ratings.drop("numVotes")
df_basics = df_basic.withColumn("decade", f.floor(f.col("startYear") / decade).cast(t.IntegerType()))
df_basics = df_basics.join(ratings, on="tconst").orderBy('averageRating', ascending=False)
df_basics.show()
window = Window.partitionBy("decade").orderBy(f.desc("averageRating"))
df_basics = df_basics.withColumn("Top_10", f.row_number().over(window)).where(f.col("Top_10") <= 10)
df_basics.show()

# 8. Get 10 titles of the most popular movies/series etc. by each genre.

# Function split!!
from_tsv_df_title_basics_split = from_tsv_df_title_basics.select(f.col("tconst"), f.col("originalTitle"),
                                                                 f.split("genres", ",").alias("genres_split"))
from_tsv_df_title_basics_split.printSchema()

# Function explode!!
from_tsv_df_title_basics_explode = from_tsv_df_title_basics_split.select(f.col("tconst"), f.col("originalTitle"),
                                                                         f.explode("genres_split")\
                                                                          .alias("genres_explode"))\
                                                                          .filter(f.col("genres_explode").isNotNull())

from_tsv_df_title_basics_explode.show()

from_tsv_df_title_basics_explode.orderBy("genres_explode", ascending=True).show(10)
ratings = title_ratings.drop("numVotes")

df_basic_movie = from_tsv_df_title_basics_explode.join(ratings, on="tconst").orderBy("genres_explode", ascending=True)

window = Window.partitionBy("genres_explode").orderBy(f.desc("averageRating"))
df_basic_movie = df_basic_movie.withColumn("Top_10", f.row_number().over(window)).where(f.col("Top_10") <= 10)

df_basic_movie.show(30)

#
# Linear regression

print("Linear regression")

# Preparing initial data

Minutes = from_tsv_df_title_basics.select("tconst", "runtimeMinutes").filter(f.col("runtimeMinutes").isNotNull())
Minutes_grop = Minutes.groupby(f.col("tconst")).max("runtimeMinutes")
df_movie_group_titl = Minutes_grop.join(title_ratings, on="tconst")
df_movie_group_titl = df_movie_group_titl.drop(df_movie_group_titl.tconst, df_movie_group_titl.numVotes)
df_movie_group_titl.show()
feature_columns = df_movie_group_titl.columns[:-1]

print(feature_columns)

assembler = VectorAssembler(inputCols = feature_columns, outputCol="features")
df_movie_predict = assembler.transform(df_movie_group_titl)
df_movie_predict = df_movie_predict.select(["features","averageRating"])
df_movie_predict.show()

feature_columns = df_movie_group_titl.columns[:-1]

print(feature_columns)

train, test = df_movie_predict.randomSplit([0.7, 0.3])

# Modelling

from pyspark.ml.regression import LinearRegression
lin_regression = LinearRegression(featuresCol = "features",labelCol="averageRating")
model = lin_regression.fit(train)

print("Coef:" + str(model.coefficients))
print("Intersept:" + str(model.intercept))

#Model Evaluation

evaluation_summary = model.evaluate(test)
evaluation_summary.r2
evaluation_summary.rootMeanSquaredError
evaluation_summary.meanAbsoluteError

# Prediction

predictions = model.transform(test)
predictions.select(predictions.columns[:]).show()

# ========

# Loading Stage
# Result Data is created for storing result files

# 1. Get all titles of series/movies etc. that are available in Ukrainian.

first_5_UA_title = UA_title.limit(5)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5UATitle"
first_5_UA_title.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 2. Get  the list of people’s names, who were born in the 19 th century.

first_5_df_name_19th_senture = df_name_19th_senture_short.limit(5)

path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5Name19thSenture"
first_5_df_name_19th_senture.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 3. Get titles of all movies that last more than 2 hours.

first_5_df_movie_length_2_hours = df_movie_length_2_hours.limit(5)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5MovieLength"
first_5_df_movie_length_2_hours.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 4. Get names of people, corresponding movies/series and characters they played in those films.

first_5_df_select_left = df_select_left_.limit(5)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5SelectNames"
first_5_df_select_left.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 5. Get information about how many adult movies/series etc. there are per region.
#    Get the top 100 of them from the region with the biggest count to the region with the smallest one.

first_100_df_adult_region = df_adult_region.limit(100)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first100AdultRegion"
first_100_df_adult_region.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 6. Get information about how many episodes in each TV Series. Get the top 50 of them starting from the TV Series with the biggest quantity of episodes

first_5_df_episode_group_titles = df_episode_group_titles.limit(5)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5Episode"
first_5_df_episode_group_titles.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 7. Get 10 titles of the most popular movies/series etc. by each decade.

first_5_df_basics = df_basics.limit(5)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5Basics"
first_5_df_basics.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

# 8. Get 10 titles of the most popular movies/series etc. by each genre.

first_5_df_basic_movie = df_basic_movie.limit(5)
path_name_to_save = "C:\\Users\\Maryna_N\\PycharmProjects\\pythonProject10\\ResultData\\first5BasicMovie"
first_5_df_basic_movie.write.csv(path_name_to_save,header=True, mode="overwrite",sep=r'\t')

