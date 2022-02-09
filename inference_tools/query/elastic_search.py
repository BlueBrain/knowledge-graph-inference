import requests
import urllib

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


def check_view_readiness(bucket_config, view_id, token):
    view_id = urllib.parse.quote_plus(view_id)
    url = f"{bucket_config.endpoint}/views/{bucket_config.org}/{bucket_config.proj}/{view_id}/statistics"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"}
    )
    response = r.json()
    last_event = response["lastEventDateTime"]
    last_processed_event = None
    if "lastProcessedEventDateTime" in response:
        last_processed_event = response["lastProcessedEventDateTime"]
    return last_event == last_processed_event
