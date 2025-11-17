import boto3
import json

AWS_REGION = "us-east-2"              # your secret is in us-east-2
SECRET_NAME = "storeops/bestbuy/api"  # the name you created in Secrets Manager

def get_bestbuy_api_key():
    """
    Fetch BestBuy API key stored in AWS Secrets Manager.
    Secret name: storeops/bestbuy/api
    Returns the string key.
    """
    client = boto3.client("secretsmanager", region_name=AWS_REGION)

    response = client.get_secret_value(
        SecretId="storeops/bestbuy/api"
    )

    secret_dict = json.loads(response["SecretString"])
    return secret_dict["bestbuy_api_key"]
