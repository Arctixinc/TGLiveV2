def get_version() -> str:
    """
    Returns the version details. Do not Interfere with this !

    :return: The version details in the format 'vMAJOR.MINOR.PATCH-STATE'
    :rtype: str
    """
    MAJOR = "2"
    MINOR = "1"
    PATCH = "4"
    STATE = "alpha"

    return f"v{MAJOR}.{MINOR}.{PATCH}-{STATE}"