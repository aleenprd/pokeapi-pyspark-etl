"""Defining parent class for ETL jobs."""


import inspect
from abc import ABC, abstractmethod
from typing import Dict, Union, Callable, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class ETLException(Exception):
    """Starting point for ETL specific exceptions."""

    pass


class SchemaMismatchException(ETLException):
    """Exception thrown when DataFrame schemas are not matched."""

    pass


class ETL(ABC):
    """Abstract class that outlines what it means to run a ETL job.
    This class entails that you implement the extract, transform and load methods.
    """

    def __init__(
        self, spark: SparkSession, configuration: Dict[str, Union[str, int, bool]]
    ) -> None:
        """Initialized a new instance of the ETL class.

        Args:
            spark (SparkSession): The spark session to use for the job.
            configuration (Dict[str, Union[str, int, bool]]): dictionary which passes inputs for ETL job;
            there is no need to pass any inputs to the methods directly.
        """
        self.spark = spark
        self.configuration = configuration

    @abstractmethod
    def extract(self) -> None:
        pass

    @abstractmethod
    def transform(self) -> None:
        pass

    @abstractmethod
    def load(self) -> None:
        pass

    def validate_extract(self) -> None:
        "Optional extra checks if needed."
        pass

    def validate_transform(self) -> None:
        "Optional extra checks if needed."
        pass

    def validate_load(self) -> None:
        "Optional extra checks if needed."
        pass

    def run_task(self) -> None:
        """Will run all of the ETL methods."""
        self.extract()
        self.validate_extract()

        self.transform()
        self.validate_transform()

        self.load()
        self.validate_load()

    @staticmethod
    def schema_strict_match(dataframe: DataFrame, required_schema: StructType) -> None:
        """Verifies that a supplied DataFrame matches strictly with the supplied schema.

        Args:
            dataframe (DataFrame): the spark DataFrame to verify
            required_schema (StructType[StructField, Any, Boolean]): the schema with the data types we expect,
            & nullability of the column.
        Raises:
            SchemaInvariantException
        """
        all_struct_fields = dataframe.schema
        missing_struct_fields = [
            x for x in required_schema if x not in all_struct_fields
        ]
        error_message = f"\n\n{missing_struct_fields} \n\nThe above StructFields are not included in the DataFrame with the following StructFields: \n\n{all_struct_fields}"
        if missing_struct_fields:
            raise SchemaMismatchException(error_message)

    @staticmethod
    def inject_configuration(required_keys: List[str]) -> Callable:
        """Decorator that inject configuration keys from the ETL configuration object into a method.
        If the key does not exist, it will throw an exception if a default argument is not specified.
        Args:
            required_keys (List[str]): List of configuration keys you wish to inject into the method. If any of them do
                not exist on the configuration object of the ETL subclass, a KeyError is thrown unless it is defined as
                a default argument.
        Returns:
            Callable: a staticmethod that is callable
        """

        def _inject_configuration(f):
            def __inject_configuration(self):
                # Fetch signature of function f
                # Example: def foo(a:int=1, b:float=2.): return a+b
                # <Signature (a: int = 1, b: float = 2.0)>
                signature = inspect.signature(f)

                # Fetch default values of parameters of function f, excluding self.
                # Example: {'a': 1, 'b': 2}
                defaults = {
                    required_keys[i]: signature.parameters[key].default
                    for i, key in enumerate(
                        [
                            parameter
                            for parameter in signature.parameters
                            if parameter != "self"
                        ]
                    )
                    if signature.parameters[key].default != inspect._empty
                }

                # Fetch any missing keys not found
                # either in configuration or required parameters list
                missing_keys = [
                    f"'{key}'"
                    for key in required_keys
                    if key not in self.configuration and key not in defaults
                ]

                # If there're any missing keys, raise a KeyError
                if len(missing_keys) > 0:
                    raise KeyError(
                        f"{type(self).__name__}.{f.__name__}: missing required configuration key(s) {', '.join(missing_keys)}"
                    )

                # Returns the result of the decorated function using the configuration
                # parameters which were provided, or in case they're missing, use the default values.
                return f(
                    self,
                    *[
                        self.configuration[key]
                        if key in self.configuration
                        else defaults[key]
                        for key in required_keys
                    ],
                )

            return __inject_configuration

        return _inject_configuration
