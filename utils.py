import base64
import json


def load_config(path_to_json):
    with open(path_to_json, "r") as f:
        return json.load(f)


def encode_to_b64_string(string):
    """ Generate a base64 string

    Utility function to encode string to base64 for URL support

    :param string: a string
    :type string: str
    :return: the name of the currently active user
    :rtype: str

    """
    string_bytes = string.encode("utf-8")
    string_b64_bytes = base64.b64encode(string_bytes)
    string_b64 = string_b64_bytes.decode("utf-8")
    return string_b64
