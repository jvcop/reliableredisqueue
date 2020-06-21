import os

_HERE = os.path.dirname(__file__)


def _load_script(name):
    path = os.path.join(_HERE, f"{name}.lua")
    with open(path) as file:
        return file.read()


GET = _load_script("get")
ACK = _load_script("ack")
FAIL = _load_script("fail")
