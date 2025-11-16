import requests
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("BESTBUY_API_KEY")

url = "https://api.bestbuy.com/v1/products"
params = {
    "format": "json",
    "show": "sku,name,regularPrice,salePrice,categoryPath.name",
    "pageSize": 5,
    "apiKey": api_key
}

resp = requests.get(url, params=params)
print("Status Code:", resp.status_code)

data = resp.json()
print("Keys in response:", data.keys())
print("First item:", data.get("products", [{}])[0])
print("Total products found:", data.get("total", 0))
print("Number of products returned:", len(data.get("products", [])))
print("Response Metadata:", {k: data[k] for k in data if k not in ["products", "total"]})