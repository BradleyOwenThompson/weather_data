from abc import ABC, abstractmethod
import os

"""Abstract base class for writing data to various destinations.
This module defines the Writer class and its concrete implementations for local file writing
and Azure Blob Storage writing.
"""


class Writer(ABC):
    """Abstract base class for writing data to various destinations.

    This class defines the interface for writing data, which can be implemented by different
    concrete writer classes such as LocalWriter and AzureBlobWriter.
    """

    @abstractmethod
    def write(self, data: str, destination: str):
        """Write data to the specified destination.

        Args:
            data (str): The data to write.
            destination (str): The destination where the data will be written.
        """


class LocalWriter(Writer):
    """Concrete implementation of Writer for writing data to local files.

    This class implements the write method to save data to a local file system.
    """

    def write(self, data: str, destination: str) -> bool:
        """Write data to a local file.

        Args:
            data (str): The data to write.
            destination (str): The local file path where the data will be written.
        """
        os.makedirs(
            os.path.dirname(destination), exist_ok=True
        )  # Ensure the directory exists

        try:
            with open(destination, "w") as file:
                file.write(data)
        except IOError as e:
            raise IOError(f"Failed to write to {destination}: {e}")
        print(f"Data written to {destination}")
        return True


class AzureBlobWriter(Writer):
    """Concrete implementation of Writer for writing data to Azure Blob Storage.

    This class implements the write method to save data to Azure Blob Storage using connection string.
    Note: This implementation is a placeholder and needs to be completed.
    """

    def __init__(self, connection_string: str):
        # TO IMPLEMENT
        pass

    def write(self, data: str, destination: str):
        # TO IMPLEMENT
        pass
