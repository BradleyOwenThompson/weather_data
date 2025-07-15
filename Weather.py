import os
import requests
from typing import Any, Literal


class WeatherData:
    """A class to interact with the Weather API"""

    def __init__(self) -> None:
        """Initialize the WeatherData class and load the API key from environment variables.

        This constructor loads the API key from a .env file using the load_dotenv function.
        The API key is expected to be stored in an environment variable named 'WEATHER_API_KEY'.

        Raises:
            ValueError: If the 'WEATHER_API_KEY' environment variable is not found.
        """
        self.API_KEY = os.getenv("WEATHER_API_KEY")
        if not self.API_KEY:
            raise ValueError("WEATHER_API_KEY not found in environment variables.")
        self.BASE_URL = os.getenv("WEATHER_API_BASE_URL")

    def get_forecast_history(self, date: str, location: str) -> tuple:
        """Retrieve historical weather data for a specific date and location.

        This method constructs the endpoint URL using the provided date and location,
        sends a GET request to the Weather API, and returns the historical weather data.

        Args:
            date (str): The date for which to retrieve historical weather data, formatted as 'YYYY-MM-DD'.
            location (str): The location for which to retrieve historical weather data, such as a city name, postal code, Latitude/Longitude (decimal degree).

        Returns:
            dict: A dictionary containing the historical weather data for the specified date and location.
        """
        endpoint_url = (
            f"{self.BASE_URL}history.json?key={self.API_KEY}&q={location}&dt={date}"
        )
        return self.__send_request(request_url=endpoint_url)

    def get_forecast_future(
        self,
        location: str,
        days: int,
        alerts: Literal["yes", "no"],
        air_quality_data: Literal["yes", "no"],
    ) -> tuple[dict[Any, Any], int]:
        """_summary_

        Args:
            location (str): The location, such as a city name, postal code, Latitude/Longitude (decimal degree).
            days (int): The number of days to forecast, must be greater than 1._
            alerts (Literal["yes", "no"]): Whether to include weather alerts in the response.
            air_quality_data (Literal["yes", "no"]): Whether to include air quality data in the response.

        Raises:
            ValueError: If 'days' is less than 1.
            ValueError: If 'alerts' is not 'yes' or 'no'.
            ValueError: If 'air_quality_data' is not 'yes' or 'no'.

        Returns:
            dict: A dictionary containing the weather forecast for the specified location and number of days.
        """

        if days < 1:
            raise ValueError("Invalid value for 'days': must be greater than 0.")

        alerts_normalized = alerts.lower()
        if alerts_normalized not in ("yes", "no"):
            raise ValueError("Invalid value for 'alerts': must be 'yes' or 'no'.")

        air_quality_data_normalized = alerts.lower()
        if air_quality_data_normalized not in ("yes", "no"):
            raise ValueError(
                "Invalid value for 'air_quality_data': must be 'yes' or 'no'."
            )

        endpoint_url = f"{self.BASE_URL}forecast.json?key={self.API_KEY}&q={location}&days={days}&aqi={air_quality_data}&alerts={alerts}"
        return self.__send_request(request_url=endpoint_url)

    def __send_request(self, request_url: str) -> tuple[dict[Any, Any], int]:
        """Send a GET request to the specified URL and return the JSON response.

        This method is a private helper function that sends a GET request to the provided URL
        and returns the JSON response as a dictionary.

        Args:
            request_url (str): The URL to which the GET request will be sent.

        Raises:
            requests.RequestException: If there is an error during the request.

        Returns:
            dict: The JSON response from the GET request, parsed into a dictionary.
        """
        try:
            response = requests.get(request_url)
            response.raise_for_status()
            return response.json(), response.status_code
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return {}, response.status_code
