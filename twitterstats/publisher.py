import json
import urllib.request
import urllib.parse


class Publisher:
    @staticmethod
    def publish(url, data, type_, account):
        payload = {}
        for k, v in data.items():
            payload[k] = [x.publish_dict() for x in v]
        payload_json = json.dumps(payload)
        values = {'data': payload_json,
                  'type': type_,
                  'account': account}
        print(values)
        data = urllib.parse.urlencode(values).encode('utf-8')
        response = urllib.request.urlopen(url, data)
        the_page = response.read()
        print(the_page)
