"""Example Sink: print each payload with its stable Delivery key."""

import json


def send(config, payload, delivery_key):
    del config
    print(json.dumps({"delivery_key": delivery_key, "payload": payload}, ensure_ascii=False))
    return delivery_key
