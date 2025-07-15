from typing import Any
from Weather import WeatherData
from Writer import Writer
from WriterFactory import get_writer
from datetime import datetime
import duckdb
from dotenv import load_dotenv
import os

"""WeatherPipeline module for managing weather data locations and forecasts.

This module provides a singleton class `WeatherPipeline` that manages the weather data pipeline,
including adding and removing locations, retrieving locations, and fetching and writing forecast history.
It uses DuckDB for database management and supports various writers for output.
"""


class SingletonMeta(type):
    """A metaclass for creating singleton classes."""

    _instances: dict[Any, Any] = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        If the class has not been instantiated yet, create a new instance.
        If it has, return the existing instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class WeatherPipeline(metaclass=SingletonMeta):
    """A class to handle the weather data pipeline."""

    def __init__(self) -> None:
        """Initialize the WeatherPipeline class and set up the database connection.

        DuckDB is used to manage the weather data locations.

        This constructor initializes the database connection and creates the necessary table
        for storing weather locations if it does not already exist.
        """
        self.con = duckdb.connect(database=os.getenv("DUCK_DB_PATH"))
        self.con.sql(
            """
            CREATE TABLE IF NOT EXISTS weather_location (
                postcode VARCHAR UNIQUE NOT NULL
                );
            """
        )
        self.weather_data = WeatherData()

    def add_location(self, postcode: str) -> None:
        """Add a new location to the weather data pipeline.

        Args:
            postcode (str): The postcode of the location to add.

        Raises:
            Exception: If the location cannot be added due to a database error.
        """
        try:
            self.con.sql(
                """
                INSERT INTO weather_location (postcode)
                VALUES (?);
                """,
                postcode,
            )
        except duckdb.DuckDBException as e:
            raise Exception(f"Failed to add location {postcode}: {e}")
        print(f"Location {postcode} added successfully.")

    def remove_location(self, postcode: str) -> None:
        """Remove a location from the weather data pipeline.

        Args:
            postcode (str): The postcode of the location to remove.

        Raises:
            Exception: If the location cannot be removed due to a database error.
        """
        try:
            self.con.sql(
                """
                DELETE FROM weather_location
                WHERE postcode = ?;
                """,
                postcode,
            )
        except duckdb.DuckDBException as e:
            raise Exception(f"Failed to remove location {postcode}: {e}")
        print(f"Location {postcode} removed successfully.")

    def get_locations(self) -> list:
        """Retrieve all locations from the weather data pipeline.

        Returns:
            list: A list of postcodes for the locations stored in the database.

        Raises:
            Exception: If the locations cannot be retrieved due to a database error.
        """
        try:
            forecast_locations = self.con.sql(
                "SELECT postcode FROM weather_location"
            ).fetchall()

            print(f"Locations retrieved: len({forecast_locations})")
            return [row[0] for row in forecast_locations]
        except duckdb.DuckDBException as e:
            raise Exception(f"Failed to retrieve locations: {e}")

    def get_and_write_forecast_history(
        self, date: str, location: str, writer: Writer, destination: str
    ) -> None:
        """Fetch and write the weather forecast history for a specific date and location.

        Args:
            date (str): The date for which to fetch the forecast history.
            location (str): The location for which to fetch the forecast history.
            writer (Writer): An instance of the Writer class to handle writing data.
            destination (str): The file path where the data should be written.

        Raises:
            Exception: If the data cannot be fetched or written successfully.
        """

        result, status_code = self.weather_data.get_forecast_history(
            date=date, location=location
        )

        if status_code != 200:
            raise Exception(f"Failed to fetch data: {result}")

        writer.write(str(result), destination)
        print(f"Data written to {destination} successfully.")


if __name__ == "__main__":
    """Main entry point for the WeatherPipeline script.

    This script initializes the WeatherPipeline, retrieves the locations, and fetches
    the weather forecast history for today for each location, writing the results to files.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Initialize the WeatherPipeline and Writer
    weather_pipeline = WeatherPipeline()
    writer = get_writer("local")

    # Retrieve the list of locations from the database
    locations = weather_pipeline.get_locations()

    # Fetch and write the forecast history for each location
    today = datetime.now().strftime("%Y-%m-%d")
    for location in locations:
        weather_pipeline.get_and_write_forecast_history(
            date=today,
            location=location,
            writer=writer,
            destination=f"./output/{location}/{today}.json",
        )

    print("Weather data pipeline executed successfully.")
