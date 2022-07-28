import base64
import json


def load_config(path_to_json):
    """Load a JSON configuration profile.

    :param path_to_json: The path to the JSON config file

    :returns: A JSON with application config
    """
    with open(path_to_json, "r") as f:
        return json.load(f)
