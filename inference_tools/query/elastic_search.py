import copy
from urllib.parse import quote_plus


def set_elastic_view(forge, view):
    views_endpoint = "/".join((
        forge._store.endpoint,
        "views",
        quote_plus(forge._store.bucket.split("/")[0]),
        quote_plus(forge._store.bucket.split("/")[1])))
    forge._store.service.elastic_endpoint["endpoint"] = "/".join(
        (views_endpoint, quote_plus(view), "_search"))


def get_all_documents(forge):
    return forge.elastic("""
        {
          "query": {
              "term": {
                  "_deprecated": false
                }
            }
        }
    """)
