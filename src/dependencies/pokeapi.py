"""Class that implements the PokeAPI ETL job."""


import os
from dataclasses import dataclass, field
from typing import Dict, List, Union
from multiprocessing.pool import Pool

# from multiprocessing.pool import ThreadPool
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.ml.feature import HashingTF, IDF

from dependencies.etl import ETL
from dependencies.objects import RequestHandler


@dataclass(frozen=True, slots=True)
class Object:
    type: str = field(init=True, repr=True)
    data: Dict[str, Union[int, float, List[Union[int, float, str]]]] = field(
        default_factory=dict, init=True, repr=True
    )


@dataclass(frozen=True, slots=True)
class Dataset:
    type: str = field(init=True, repr=True)
    data: DataFrame = field(default_factory=DataFrame, init=True, repr=True)


class PokeAPI(ETL):
    """Class extending ETL abstract which aggregates PokeAPI objects.

    Note: Class implements 3 methods: extract(), transform() and load(),
    together with validations, through a simple call to the run_task() method.
    """

    def __init__(
        self, spark: SparkSession, configuration: Dict[str, Union[str, int, bool]]
    ) -> None:
        super().__init__(spark, configuration)
        self.__supported_obj = ["pokemon", "generation", "evolution"]

    @staticmethod
    def join_base_datasets(
        df_pokemon: DataFrame, df_generation: DataFrame, df_evolution: DataFrame
    ) -> DataFrame:
        """Join the Pokemon and Generation datasets."""
        df_generation = df_generation.select(
            [
                F.col("id").alias("generation_id"),
                F.explode(F.col("species")).alias("species"),
            ]
        )

        df_evolution = (
            df_evolution.withColumn("specie", F.explode(F.col("species")))
            .drop(F.col("species"))
            .withColumnRenamed("id", "evolution_id")
        )

        df_pokemon = (
            df_pokemon.alias("p")
            .join(
                df_generation.alias("s"),
                F.col("p.name") == F.col("s.species"),
                how="inner",
            )
            .select(
                [F.col(f"p.{x}") for x in df_pokemon.columns]
                + [F.col("s.generation_id")]
            )
        )

        df_pokemon = (
            df_pokemon.alias("p")
            .join(
                df_evolution.alias("e"),
                F.col("p.name") == F.col("e.specie"),
                how="left",
            )
            .select(
                [F.col(f"p.{x}") for x in df_pokemon.columns]
                + [F.col("e.evolution_id")]
            )
        )

        # Many Pokemons don't have a generation or an evolution, so we will cut them out
        df_pokemon = df_pokemon.dropna()  # (saves a lot of computing)

        return df_pokemon.drop_duplicates()

    @staticmethod
    def create_generation_stats_df(df_pokemon: DataFrame) -> DataFrame:
        """Calculate a dataframe of average Pokemon base stats, grouped by generation."""
        stat_types = [
            "hp",
            "attack",
            "defense",
            "special-attack",
            "special-defense",
            "speed",
        ]

        for stat in range(0, len(stat_types)):
            df_pokemon = df_pokemon.withColumn(
                f"stat_{stat_types[stat]}".replace("-", "_"), F.col("stats")[stat][1]
            )

        df_pokemon = df_pokemon.drop(F.col("stats"))

        # Calculate the average stats value for each individual Pokemon
        stat_types = [f"stat_{s}".replace("-", "_") for s in stat_types]
        df_pokemon = df_pokemon.withColumn(
            "avg_all_stats", sum(F.col(s) for s in stat_types) / F.lit(len(stat_types))
        )

        # Now we want to know the average stats across all generations (bonus)? And the average per each stat?
        aggregate_expression = {x: "avg" for x in ["avg_all_stats"] + stat_types}

        return (
            df_pokemon.groupBy("generation_id")
            .agg(aggregate_expression)
            .orderBy(F.col("generation_id"))
        )

    @staticmethod
    def create_generation_coverage_df(df_pokemon: DataFrame) -> DataFrame:
        """Calculate the Pokemon coverage and group it per generation."""
        # A coverage below 1 means there are more types than abilities
        # A coverage of 1 means that each ability has its type
        # A coverage greater than 1 means that the pokemon has on average more than 1 ability for each type
        # The higher the score, the greater the 'coverage'

        df_pokemon = df_pokemon.withColumn("num_abilities", F.size(F.col("abilities")))

        df_pokemon = df_pokemon.withColumn("num_types", F.size(F.col("types")))

        df_pokemon = df_pokemon.withColumn(
            "coverage", F.col("num_types") / F.col("num_abilities")
        )

        return (
            df_pokemon.groupBy("generation_id")
            .agg(F.mean("coverage").alias("avg_coverage"))
            .orderBy(F.col("generation_id"))
        )

    @staticmethod
    def create_hidden_ability_df(df_pokemon: DataFrame) -> DataFrame:
        """Determine for each Pokemon which abilities are hidden and which are not?"""
        df_pokemon = df_pokemon.select(
            F.col("name"), F.explode(F.col("abilities")).alias("ability")
        ).withColumn("ability_name", F.col("ability")[0])

        df_pokemon = df_pokemon.withColumn("ability_hidden", F.col("ability")[1]).drop(
            F.col("ability")
        )

        df_ability_hidden_true = (
            df_pokemon.where(F.col("ability_hidden") == True)
            .drop(F.col("ability_hidden"))
            .orderBy(F.col("ability_name"))
        )

        df_ability_hidden_true = df_ability_hidden_true.groupBy(
            F.col("ability_name")
        ).agg(F.collect_set(F.col("name")).alias("hidden"))

        df_ability_hidden_false = (
            df_pokemon.where(F.col("ability_hidden") == False)
            .drop(F.col("ability_hidden"))
            .orderBy(F.col("ability_name"))
        )

        df_ability_hidden_false = df_ability_hidden_false.groupBy(
            F.col("ability_name")
        ).agg(F.collect_set(F.col("name")).alias("not_hidden"))

        return (
            df_ability_hidden_true.alias("a")
            .join(
                df_ability_hidden_false.alias("b"),
                F.col("a.ability_name") == F.col("b.ability_name"),
                how="inner",
            )
            .select(
                [F.col(f"a.{x}") for x in df_ability_hidden_true.columns]
                + [F.col("b.not_hidden")]
            )
        )

    @staticmethod
    def create_cosine_similarity_df(
        df_pokemon: DataFrame, similarity_threshold: float = 0.75
    ) -> DataFrame:
        """Find Pokemon with similar move-set that do not belong to same evolution and are in different generations."""
        df_abilities = (
            df_pokemon.withColumn("ability", F.explode(df_pokemon["abilities"]))
            .withColumn("ability", F.col("ability")[0])
            .groupBy("name")
            .agg(F.collect_list("ability").alias("abilities"))
        )

        df_pokemon = (
            df_pokemon.drop(F.col("stats"))
            .drop(F.col("types"))
            .drop(F.col("abilities"))
        )

        df_pokemon = (
            df_pokemon.alias("a")
            .join(
                df_abilities.alias("b"),
                F.col("a.name") == F.col("b.name"),
                how="inner",
            )
            .select(
                [F.col(f"a.{x}") for x in df_pokemon.columns] + [F.col("b.abilities")]
            )
        ).drop_duplicates()

        # First, we want to calculate the TF-IDF matrix
        hashingTF = HashingTF(inputCol="abilities", outputCol="tf")
        df_tf = hashingTF.transform(df_pokemon)

        idf = IDF(inputCol="tf", outputCol="feature").fit(df_tf)
        df_tfidf = idf.transform(df_tf)

        @F.udf
        def cosime_similarity(v1: List, v2: List) -> float:
            """Calculate cosime similarity between two vectors."""
            p = 2
            return float(v1.dot(v2)) / float(v1.norm(p) * v2.norm(p))

        # Then we do an expensive Cartesiań Join
        df_cosine_similarity = (
            df_tfidf.alias("i")
            .crossJoin(df_tfidf.alias("j"))
            .select(
                F.col("i.name").alias("name_i"),
                F.col("j.name").alias("name_j"),
                F.col("i.evolution_id").alias("evolution_id_i"),
                F.col("j.evolution_id").alias("evolution_id_j"),
                F.col("i.generation_id").alias("generation_id_i"),
                F.col("j.generation_id").alias("generation_id_j"),
                cosime_similarity("i.feature", "j.feature").alias("sim_cosine"),
            )
        )

        df_cosine_similarity = (
            df_cosine_similarity.withColumn(
                "same_evolution_chain",
                F.col("evolution_id_i") == F.col("evolution_id_j"),
            )
            .drop("evolution_id_i")
            .drop("evolution_id_j")
        )

        df_cosine_similarity = (
            df_cosine_similarity.withColumn(
                "same_generation", F.col("generation_id_i") == F.col("generation_id_j")
            )
            .drop("generation_id_i")
            .drop("generation_id_j")
        )

        df_cosine_similarity = df_cosine_similarity.where(
            F.col("sim_cosine") > similarity_threshold
        )

        df_cosine_similarity = df_cosine_similarity.where(
            F.col("same_evolution_chain") == False
        ).drop("same_evolution_chain")

        return df_cosine_similarity.where(F.col("same_generation") == False).drop(
            "same_generation"
        )

    @ETL.inject_configuration(["extract_save_path"])
    def extract(self, extract_save_path: Union[None, str]) -> None:
        """Extracts data from API, transforms it and loads it to a location.
        Note: Actually an ETL in itself. We load the response from each request, such as to not
        load all the data at the same time and possibly overload the driver. The processing makes
        use of driver parallelism with multiprocessing.
        Args:
            extract_save_path (str): base folder for saving data. Different files will be
            automatically saved at deterministic paths. Defaults to None. If None, the objects are loaded in RAM.
        """
        print("\nExtracting Stage...")
        # First we want to fetch the IDs of all objects
        print("\nFetching ID lists...")
        id_lists = {
            k: RequestHandler.get_all_objects_ids(k) for k in self.__supported_obj
        }

        # Then we want to make a get request for each object, process it, and write it immediately
        # pool_t = ThreadPool()
        pool_m = Pool()
        self.objects = []

        print("\nFetching data based on IDs...")
        for k in self.__supported_obj:
            print(f"* {k}")
            parameters = list(
                zip(
                    id_lists[k],
                    [k] * len(id_lists[k]),
                    [extract_save_path] * len(id_lists[k]),
                )
            )
            data = pool_m.starmap(RequestHandler.parse_pokeapi_object, parameters)
            self.objects.append(Object(type=k, data=data))

        # Finally, we can create our Spark dataframes either from RAM or from Disk
        print("\nCreating Spark dataframes...")

        self.datasets = []
        for o in self.objects:
            print(f"* {o.type}")

            if o.data[0]:
                self.datasets.append(
                    Dataset(type=o.type, data=self.spark.createDataFrame(o.data))
                )
            else:
                paths = [
                    f"{extract_save_path}/{o.type}/{p}"
                    for p in os.listdir(f"{extract_save_path}/{o.type}")
                ]
                data = self.spark.read.option("multiline", "true").json(paths)
                self.datasets.append(
                    Dataset(
                        type=o.type,
                        data=data,
                    )
                )

    @ETL.inject_configuration(["cosine_threshold"])
    def transform(self, cosine_threshold: int) -> None:
        """Transforms the data, such that it can be used by analytical users.
        Args:
            cosine_threshold (int): threshold for calculating similarity scores.
        """
        print("\nTransforming Stage...")
        df_pokemon = [x for x in self.datasets if x.type == "pokemon"][0].data
        df_generation = [x for x in self.datasets if x.type == "generation"][0].data
        df_evolution = [x for x in self.datasets if x.type == "evolution"][0].data

        print("* Pokemon Dataframe:")
        df_pokemon.show()
        print("* Generation Dataframe:")
        df_generation.show()
        print("* Evolution Dataframe:")
        df_evolution.show()

        print("* Joining bases Dataframes into:")
        df_pokemon = PokeAPI.join_base_datasets(df_pokemon, df_generation, df_evolution)
        df_pokemon.show()

        print("\nCreating Generation Stats...")
        df_generation_stats = PokeAPI.create_generation_stats_df(df_pokemon)
        df_generation_stats.show()
        self.datasets.append(
            Dataset(
                type="generation_stats",
                data=df_generation_stats,
            )
        )
        print("\nCreating Generation Coverage...")
        df_generation_coverage = PokeAPI.create_generation_coverage_df(df_pokemon)
        df_generation_coverage.show()
        self.datasets.append(
            Dataset(
                type="generation_coverage",
                data=df_generation_coverage,
            )
        )
        print("\nCreating Hidden Ability...")
        df_hidden_ability = PokeAPI.create_hidden_ability_df(df_pokemon)
        df_hidden_ability.show()
        self.datasets.append(
            Dataset(
                type="hidden_ability",
                data=df_hidden_ability,
            )
        )
        print("\nCreating Cosine Similarity...")
        df_cosine_similarity = PokeAPI.create_cosine_similarity_df(
            df_pokemon, cosine_threshold
        )
        df_cosine_similarity.show()
        self.datasets.append(
            Dataset(
                type="cosine_similarity",
                data=df_cosine_similarity,
            )
        )

    @ETL.inject_configuration(["load_save_path"])
    def load(self, load_save_path: str) -> None:
        """Saves the datasets at a specified location."""
        print("\nLoading Stage...")
        for d in self.datasets:
            print(f"* {d.type}")
            (d.data).write.format("parquet").mode("overwrite").save(
                f"{load_save_path}/{d.type}"
            )


# def get_all_columns_from_pyspark_schema(
#     schema: StructType, depth: Union[None, int] = None
# ) -> List[str]:
#     """Get column names from Pyspark.sql.dataframe schema.
#     Args:
#         schema (pyspark.sql.types.StructType): schema of dataframe.
#         depth (None): depth of tree traversal. Defaults to 0.
#     Return:
#         (List[str]): list of column names.
#     """
#     result = []
#     if depth is None:
#         depth = 0
#     for field in schema.fields:
#         result.append(field.name)
#         if isinstance(field.dataType, StructType):
#             get_all_columns_from_pyspark_schema(field.dataType, depth + 1)

#     return result
