import base64


def encode_to_b64_string(*args):
    string = ""
    for x in args:
        string += x
    string_bytes = string.encode("utf-8")
    string_b64_bytes = base64.b64encode(string_bytes)
    string_b64 = string_b64_bytes.decode("utf-8")
    return string_b64
