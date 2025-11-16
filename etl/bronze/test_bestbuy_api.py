import boto3
import json
import requests


SECRET_NAME = "storeops/bestbuy/api"   
REGION_NAME = "us-east-2"              


def get_bestbuy_api_key():
    client = boto3.client("secretsmanager", region_name=REGION_NAME)
    resp = client.get_secret_value(SecretId=SECRET_NAME)
    secret_dict = json.loads(resp["SecretString"])
    return secret_dict["bestbuy_api_key"]


def main():
    api_key = get_bestbuy_api_key()
    print("Got API key from Secrets Manager ✅")

    url = "https://api.bestbuy.com/v1/products"
    params = {
        "format": "json",
        "show": "sku,name,regularPrice,salePrice,categoryPath.name",
        "pageSize": 5,
        "apiKey": api_key,
    }

    resp = requests.get(url, params=params, timeout=30)
    print("Status code:", resp.status_code)

    data = resp.json()
    products = data.get("products", [])

    print("Number of products returned:", len(products))
    if products:
        print("First product sample:")
        print(json.dumps(products[0], indent=2))


if __name__ == "__main__":
    main()
