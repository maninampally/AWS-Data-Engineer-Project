import boto3
import json

def get_bestbuy_api_key():
    """
    Fetch BestBuy API key stored in AWS Secrets Manager.
    Secret name: storeops/bestbuy/api
    Returns the string key.
    """
    client = boto3.client("secretsmanager")

    response = client.get_secret_value(
        SecretId="storeops/bestbuy/api"
    )

    secret_dict = json.loads(response["SecretString"])
    return secret_dict["bestbuy_api_key"]
