import re
import requests
import Levenshtein
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, avg
from pyspark.sql.types import IntegerType, BooleanType
from nltk.stem import PorterStemmer

def levenshtein_distance(s1, s2):
    return Levenshtein.distance(s1, s2)

def stem_word(word):
    stemmer = PorterStemmer()
    return stemmer.stem(word)

def is_variation(word, target_word, specific_variations, fuzzy_threshold):
    stemmed_target_word = stem_word(target_word)
    words = word.split()
    for word in words:
        if word.lower() in specific_variations:
            stemmed_word = stem_word(word)
            if levenshtein_distance(stemmed_word, stemmed_target_word) <= fuzzy_threshold:
                return True
    return False

def extract_minutes(duration):
    if duration is not None and duration.startswith("PT"):
        time_expr = duration[2:]
        if time_expr is not None:
            hours_match = re.search(r'(\d+)H', time_expr)
            minutes_match = re.search(r'(\d+)M', time_expr)
            seconds_match = re.search(r'(\d+)S', time_expr)
            hours = int(hours_match.group(1)) if hours_match else 0
            minutes = int(minutes_match.group(1)) if minutes_match else 0
            seconds = int(seconds_match.group(1)) if seconds_match else 0
            return hours * 60 + minutes + round(seconds / 60)
        else:
            return 0

def extract_recipes(raw_recipes_df, target_word='chilies', specific_variations=None, fuzzy_threshold=2):
    if specific_variations is None:
        specific_variations = ["chillies", "chilies", "chilis", "chilly", "challis", "challies"]
    udf_is_variation = udf(lambda word: is_variation(word, target_word, specific_variations, fuzzy_threshold), BooleanType())
    raw_recipes_df = raw_recipes_df.withColumn('is_variation', udf_is_variation(col("ingredients")))
    filtered_df = raw_recipes_df.filter(col('is_variation'))
    final_df = filtered_df.drop('is_variation')
    return final_df

def add_difficulty_level(int_recipes_df):
    udf_extract_minutes = udf(extract_minutes, IntegerType())
    int_recipes_df = int_recipes_df.withColumn('cookTimeMinutes', udf_extract_minutes(col('cookTime')))
    int_recipes_df = int_recipes_df.withColumn('prepTimeMinutes', udf_extract_minutes(col('prepTime')))
    df_with_total_mins = int_recipes_df.withColumn('totalTimeMinutes', col('cookTimeMinutes') + col('prepTimeMinutes'))
    df_with_difficulty_level = df_with_total_mins.withColumn('Difficulty', when(col('totalTimeMinutes') > 60, "Hard").when((col('totalTimeMinutes') <= 60) & (col('totalTimeMinutes') > 30), "Medium").when(col('totalTimeMinutes') <= 30, "Easy").otherwise("Unknown"))
    df_with_difficulty_level = df_with_difficulty_level.drop('cookTimeMinutes', 'prepTimeMinutes')
    df_with_difficulty_level = df_with_difficulty_level.dropDuplicates()
    df_with_difficulty_level.write.csv("chilies.csv", sep="|", header=True)
    return df_with_difficulty_level

def aggregate_total_minutes(df_difficulty_level):
    avg_df = df_difficulty_level.groupBy("Difficulty").agg(avg(col("totalTimeMinutes")).alias("AverageTotalTime"))
    result_rows = avg_df.rdd.map(lambda row: f"{row['Difficulty']}|AverageTotalTime|{row['AverageTotalTime']}")
    result_rows.saveAsTextFile("results.csv")

def process(url, local_filename):
    spark = SparkSession.builder.appName('TransformRecipesData').getOrCreate()
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    df = spark.read.format("json").load(local_filename)
    df_extract_chilies = extract_recipes(df)
    df_with_difficulty_level = add_difficulty_level(df_extract_chilies)
    aggregate_total_minutes(df_with_difficulty_level)

if __name__ == '__main__':
    url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    local_filename = "recipes.json"
    process(url, local_filename)
