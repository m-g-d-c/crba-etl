
import requests

# function for api request as proposed by Daniele
def api_request(address, params=None, headers=None):
    try:
        response = requests.get(address, params=params, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except Exception as error:
        print(f"Other error occurred: {error}")
        raise
    # return response object
    return response