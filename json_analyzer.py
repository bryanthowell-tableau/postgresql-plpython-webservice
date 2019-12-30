import requests

# Hit a rest API endpoint
rest_endpoint = 'https://myservice/endpoint'
start_date = '2017-01-01'
end_date = '2019-12-31'
api_key = 'myAPIkey'

api_params = {'start_date' : start_date, 'end_date' : end_date, 'api_key': api_key}
response = requests.get(url=rest_endpoint, params=api_params)

# Some alternative possibilities depending on what your API needs (https://requests.readthedocs.io/en/master/user/quickstart/)
# api_headers = { 'api_token' : api_token}
# response = requests.get(url=rest_endpoint, params=api_params, headers=api_headers)
# This could be requests.post with a payload if the API needs POST methods rather than GET
# response = requests.post(url=rest_endpoint, data=api_params)

# Check for a valid response, if not raise an Exception
if response.status_code >= 400:
    response.raise_for_status()
# Grab the JSON from the response (if your API is returning plain-text or XML, use response.text)
rest_json = response.json()
# response_text = response.text
print(rest_json)
for first_level in rest_json:
    data_in_first_level = rest_json[first_level]
    # Get all the keys listed out
    print(data_in_first_level.keys())

    # In this example, there are many keys to pick from, but the 'data' key
    # returns a list of lists, each representing a one-dimensional row of data
    for second_level in data_in_first_level['data']:
        print(second_level)
        for col in second_level:
            print(col, type(col))

# Present any challenges with the structure -> SQL

# Present a SQL statement from table creation

# Allow for change of name and data type

# Present final table create SQL statement