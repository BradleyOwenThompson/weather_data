from Writer import LocalWriter, AzureBlobWriter, Writer


def get_writer(writer_type: str, **auth) -> LocalWriter | AzureBlobWriter:
    """Factory function to get the appropriate writer based on the type.

    Args:
        writer_type (str): The type of writer to create. Supported values are "local" and "azure".
        **auth: Additional authentication parameters for the writer, if needed.

    Returns:
        Writer: An instance of the specified writer type.

    Raises:
        ValueError: If the specified writer type is not supported.
    """
    if writer_type == "local":
        return LocalWriter()
    elif writer_type == "azure":
        # return AzureBlobWriter(auth.get("connection_string"))
        return ModuleNotFoundError("AzureBlobWriter is not implemented yet.")
    else:
        raise ValueError(f"Unknown writer type: {writer_type}")
