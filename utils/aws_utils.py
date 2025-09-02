import boto3
import json
import os
from typing import Any, Dict, Optional


def invoke_lambda(
    function_name: str,
    payload: Dict[str, Any],
    region: str = "us-east-2",
    invocation_type: str = "RequestResponse",
) -> Optional[Dict[str, Any]]:
    """
    Invoke an AWS Lambda function and return its response.

    Args:
        function_name (str): Name or ARN of the Lambda function
        payload (dict): Data to pass to the Lambda function
        region (str): AWS region where the Lambda is deployed (default: us-east-1)
        invocation_type (str): Type of invocation - 'RequestResponse' (synchronous)
                              or 'Event' (asynchronous)

    Returns:
        dict: Response from the Lambda function if invocation_type is 'RequestResponse'
        None: If invocation_type is 'Event' or if there's no response payload

    Raises:
        boto3.exceptions.Boto3Error: If there's an error invoking the function
    """
    lambda_client = boto3.client(
        "lambda",
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
        region_name=region,
    )

    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload),
        )
        # For async invocations, return None
        if invocation_type == "Event":
            return None

        # Check if there's a payload in the response
        if "Payload" in response:
            response_payload = json.loads(response["Payload"].read().decode("utf-8"))
            return response_payload

        return None

    except Exception as e:
        raise Exception(f"Error invoking Lambda function {function_name}: {str(e)}")
