"""Main module."""


import argparse
from time import time
from pyspark.sql import SparkSession
from dependencies.pokeapi import PokeAPI


parser = argparse.ArgumentParser()

parser.add_argument(
    "--extract_save_path",
    type=str,
    required=False,
    help="Path where to save list of parsed JSONs retrieved from GET methods.",
    default="../data/raw",
)

parser.add_argument(
    "--cosine_threshold",
    type=float,
    required=False,
    help="Min cosine similarity by which to filter.",
    default=0.75,
)

parser.add_argument(
    "--load_save_path",
    type=str,
    required=False,
    help="Path where to save Parquet files of analytical DataFrames.",
    default="../data/transformed",
)

parser.add_argument(
    "--spark_driver_memory",
    type=int,
    required=False,
    help="RAM on driver node.",
    default=4,
)

parser.add_argument(
    "--spark_executor_memory",
    type=int,
    required=False,
    help="RAM on executor node.",
    default=6,
)


def main(args) -> None:
    """Main function will run ETL pipeline."""
    print("\nPokeAPI ETL Spark Job running...\n")
    # Determine Spark configuration and initialize session
    spark_config = {
        "spark.driver.memory": f"{args.spark_driver_memory}g",
        "spark.executor.memory": f"{args.spark_executor_memory}g",
    }

    spark_builder = SparkSession.builder.appName("etl-pokeapi")
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    spark = spark_builder.getOrCreate()

    # Define configuration for ETL job
    configuration = {
        "extract_save_path": args.extract_save_path,
        "cosine_threshold": args.cosine_threshold,
        "load_save_path": args.load_save_path,
    }

    # Run ETL job end to end
    PokeAPI(spark, configuration).run_task()

    print("\nETL Spark Job finished.")


if __name__ == "__main__":
    t1 = time()
    args = parser.parse_args()
    main(args)
    t2 = time()
    print(t2 - t1)
