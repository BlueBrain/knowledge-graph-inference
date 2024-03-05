from typing import Dict
import json

import requests


class DeltaException(Exception):
    body: Dict
    status_code: int

    def __init__(self, body: Dict, status_code: int):
        self.body = body
        self.status_code = status_code


class DeltaUtils:

    @staticmethod
    def make_header(token):
        return {
            "mode": "cors",
            "Content-Type": "application/json",
            "Accept": "application/ld+json, application/json",
            "Authorization": "Bearer " + token
        }

    @staticmethod
    def check_response(response: requests.Response) -> Dict:
        if response.status_code not in range(200, 229):
            raise DeltaException(body=json.loads(response.text), status_code=response.status_code)
        return json.loads(response.text)

    @staticmethod
    def check_hits(response_body: Dict):
        if "hits" not in response_body or len(response_body["hits"]) == 0:
            raise DeltaException(body={}, status_code=100)  # TODO

        return response_body["hits"]["hits"]
