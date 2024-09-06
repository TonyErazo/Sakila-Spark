from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import avg
from pyspark.sql.utils import AnalysisException
import os
from dotenv import load_dotenv, dotenv_values


def get_distinct_actor_last_names(actor_df):
    distinct_actors = actor_df.select("last_name").distinct()

    # 1. How many distinct actors last names are there?
    print(f"There are a total of unique distinct names are: {distinct_actors.count()}")

def get_last_names_not_repeated(actor_df):
    # 2. Which last names are not repeated?
    grouped_last_names = actor_df.groupBy("last_name").count()
    non_repeated_last_names = grouped_last_names.filter(col("count") == 1)
    return non_repeated_last_names

def get_last_names_repeated(actor_df):
    #3. Which last names appear more than once?
    grouped_last_names = actor_df.groupBy("last_name").count()
    repeated_last_names = grouped_last_names.filter(col("count") > 1)
    return repeated_last_names

def get_avg_filmtime_all(film_df):
    # 4. What is that average running time of all the films in the sakila DB?
    avg_film_time = film_df.select(avg("length").alias("average_length"))
    return avg_film_time

def get_length_by_category(film_df, film_category_df, category_df):
    #5. What is the average running time of films by category?
    joined_film_df = film_df.join(film_category_df, "film_id")
    joined_category_df = joined_film_df.join(category_df, "category_id")

    length_by_category_df = joined_category_df.groupBy("name").agg(avg("length").alias("average_length"))
    return length_by_category_df

def submit_query(spark, url, properties, query):

    try:
        # query = "(SELECT * FROM actor) as temp"
        df = spark.read.jdbc(url=url, table=query, properties=properties)

        # result = spark.sql(query)
        return df
    except (ValueError, RuntimeError, TypeError, NameError, AnalysisException):
        print("Unable to process this query!")
    return None

def print_menu():
    print("Welcome!")
    print("The menu is as follows:")
    print("1 - How many distinct actors last names are there?")
    print("2 - Which last names are not repeated?")
    print("3 - Which last names appear more than once?")
    print("4 - What is that average running time of all the films in the sakila DB?")
    print("5 - What is the average running time of films by category?")
    print("6 - Custom SQL Query")
    print("7 - Quit")

def process_input(username, password):
    # Create Spark Session 
    spark = SparkSession.builder \
    .appName("Load SQL Database") \
    .config("spark.jars", "/opt/spark/jars/mysql-connector-j-9.0.0.jar") \
    .getOrCreate()


    url = "jdbc:mysql://localhost:3306/sakila"

    properties = {
        "user": username,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }


    # Load the 'actor' table from the sakila database
    df = spark.read.jdbc(url=url, table="actor", properties=properties)
    while True:
        print_menu()

        try:
            user_selection = int(input("Please enter your selection\n"))

            match user_selection:
                case 1:
                    # Question 1
                    get_distinct_actor_last_names(df)
                case 2:
                    # Question 2
                    non_repeated_last_names = get_last_names_not_repeated(df)
                    print("The non repeated last names are the following")
                    non_repeated_last_names.show()
                case 3:
                    # Question 3
                    repeated_last_names = get_last_names_repeated(df)
                    print("The repeated last names are the following")
                    repeated_last_names.show()
                case 4:
                    # 4. What is that average running time of all the films in the sakila DB?
                    df = spark.read.jdbc(url=url, table="film", properties=properties)
                    avg_film_time = get_avg_filmtime_all(df)
                    print(f"Average film time is: ")
                    avg_film_time.show()
                case 5:
                    #5. What is the average running time of films by category?
                    df = spark.read.jdbc(url=url, table="film", properties=properties)
                    film_category_df = spark.read.jdbc(url=url, table="film_category", properties=properties)

                    category_df = spark.read.jdbc(url=url, table="category", properties=properties)

                    length_by_category_df = get_length_by_category(df, film_category_df, category_df)
                    print("Average film time is: ")
                    length_by_category_df.show()
                case 6:
                    # query = "(SELECT * FROM actor) as temp"
                    query = input("Please enter your query\n")
                    result = submit_query(spark, url, properties, query)
                    result.show()
                case 7:
                    exit()
        except ValueError as e:
            print("Enter a number!")

            

if __name__ == "__main__":

    load_dotenv()
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")

    process_input(username, password)

    # Show the DataFrame
    # df.show()
