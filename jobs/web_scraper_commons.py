import requests
from time import time
from typing import Callable

PROXY_URL = "http://flaresolverr:8191/v1"

def retry(func: Callable):
    def wrapper(*args, **kwargs):
        resp = func(*args, **kwargs)
        while resp["status"] == "error":
            print(f"Error returned: {resp}")
            time.sleep(5)
            print("Sleeping 5s ...")
            resp = func(*args, **kwargs)
        print("Correct result")
        return resp   
    return wrapper

@retry
def get_html_content(url: str) -> str:
    data = {
        "cmd": "request.get",
        "url": url
    }
    headers = {
        "Content-Type": "application/json"
    }
    json_resp =  requests.post(PROXY_URL, json=data, headers=headers).json()
    return json_resp

def clean_url(url: str) -> str:
    return url.replace('\\"', '').replace("//", '').replace("www.", "https://")