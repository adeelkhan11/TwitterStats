import json
import urllib.request
import urllib.parse
from dataclasses import is_dataclass


class Publisher:
    @staticmethod
    def publish(env, data, type_):
        url = env.base_url + 'deliver'
        account = env.default_account
        payload = {}
        for k, v in data.items():
            if isinstance(v, list):
                value = list()
                for val in v:
                    value.append(val.publish_dict() if is_dataclass(val) else val)
            elif is_dataclass(v):
                value = v.publish_dict()
            else:
                value = v
            payload[k] = value
        payload_json = json.dumps(payload)
        values = {'data': payload_json,
                  'type': type_,
                  'account': account}
        print(values)
        data = urllib.parse.urlencode(values).encode('utf-8')
        response = urllib.request.urlopen(url, data, timeout=60)
        the_page = response.read()
        print(the_page)

    @staticmethod
    def get_pending(env):
        url = env.base_url + 'getpending?account=' + env.default_account
        response = urllib.request.urlopen(url, timeout=60)
        the_page = response.read()

        data = json.loads(the_page)
        print(data)
        return data
