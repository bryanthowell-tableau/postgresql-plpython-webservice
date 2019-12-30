# This accesses the Quandl Wiki Continuous Futures API
# https://www.quandl.com/data/CHRIS-Wiki-Continuous-Futures
# series_name is one of the API codes, whatever is after 'CHRIS/' in the documentation
# Only copy the portion below to create the actual function

CREATE OR REPLACE FUNCTION
web_service_jwt(jwt_token text)
RETURNS TABLE(like api_table_3)
AS $$

# Per Data Type Mapping documentation (https://www.postgresql.org/docs/10/plpython-data.html)
# everything non-numeric or bool comes in as a string, so start_date and end_date are declared as
# text above, expecting 'YYYYMMDD'. These work directly in PostgreSQL time functions if passed as string in single-quotes

#
# Input sanitizing: don't return anything without parameters within bounds
#

# This just makes sure things aren't blank. Dates and values might have other constraints
for arg in [jwt_token, ]:
    if arg == "" or arg is None:
        return []

# requests library is standard for making HTTP requests
import requests
import datetime
import jwt  # pyjwt library must be installed, allows parsing a JWT token
# Name of actual table created to store the responses from the Web Service
# Should start with a creation_timestamp and unique_request_id columns, then the
# flattened response from the Web Service
storage_table_name = 'api_table_3'

# REST endpoint name to be requested from
# Also might have logic here for adding in parameters to the REST request
# possibly differentiating between GET and POST requests

# This example, start_date and end_date need to be formatted YYYY-MM-DD
# Maybe add some text mangling to figure out the date formatting
quandl_api_key = ''
rest_endpoint = 'https://www.quandl.com/api/v3/datasets/CHRIS/{}/data.json'

# For caching and filter purposes, combine all input arguments into a single string
# which will be included in each row
unique_request_id = "{}".format(jwt_token)

# Cleanup old sessions - clean everything more than 24 hours old, and then assign a
# "session cleanup" that cleans out and rerequests from the same unique_request_id after a certain amount of time
all_cleanup_minutes = 60 * 24
param_key_cleanup_minutes = 30

# Only run on certain minutes of the hour
minutes_to_run_on = [0, 15, 30, 45]
right_now = datetime.datetime.now()

all_cleanup_check_query = """
SELECT MAX(creation_timestamp)
FROM {}
WHERE creation_timestamp <= NOW() + '- {} minutes'
GROUP BY unique_request_id
""".format(storage_table_name, all_cleanup_minutes)

all_cleanup_query = """
DELETE FROM {}
WHERE creation_timestamp <= NOW() + '- {} minutes'
""".format(storage_table_name, all_cleanup_minutes)

if right_now.minute in minutes_to_run_on:
    cleanup_check = plpy.execute(all_cleanup_check_query)
    if cleanup_check.nrows() > 0:
        cleanup = plpy.execute(all_cleanup_query)


# Check for existing rows with this unique_request_id. Skip the request if it already exists
# Could also include a time-bounding here -- if the data is no longer current, delete rows then request new

composite_param_old_cleanup_query = """
DELETE FROM {} 
WHERE unique_request_id = '{}'
AND creation_timestamp <= NOW() + '- {} minutes'
""".format(storage_table_name, unique_request_id, param_key_cleanup_minutes)

exists_check_query = """
SELECT MAX(creation_timestamp) FROM {} 
WHERE unique_request_id = '{}'
AND creation_timestamp >= NOW() + '- {} minutes'
GROUP BY unique_request_id
""".format(storage_table_name, unique_request_id, param_key_cleanup_minutes)

exists_check = plpy.execute(exists_check_query)

# Only rerun API request if the data doesn't already exist for this combination of arguments
if exists_check.nrows() == 0:
    # This cleans up any old ones just for this particular key
    plpy.execute(composite_param_old_cleanup_query)

    #
    # Make actual request to web service for data
    #

    # We'll parse the JWT here
    # Assume a JWT payload that looks like this (Yours will look like whatever it looks like)
    # jwt_payload = {'series_name': str,
    #              'iat': iat, # timestamp
    #               'start_date': str,
    #                'end_date' : str,
    #               'username' : str
    #               }
    jwt_secret = 'between-you-and-me-keep-this-a-secret'
    decoded_value = jwt.decode(jwt_token, jwt_secret, algorithm='HS256')
    final_rest_endpoint = rest_endpoint.format(decoded_value['series_name'])

    # If you pass times as a time type, you might have to do some more time mangling to get to string
    api_params = {'start_date' : decoded_value['start_date'], 'end_date' : decoded_value['end_date']}
    # If you have a Quandl API key, it will get placed in here
    if quandl_api_key != "":
        api_params['api_key'] = quandl_api_key
    response = requests.get(url=final_rest_endpoint, params=api_params)

    # Some alternative possibilities depending on what your API needs (https://requests.readthedocs.io/en/master/user/quickstart/)
    # api_headers = { 'api_token' : api_token}
    # response = requests.get(url=rest_endpoint, params=api_params, headers=api_headers)
    # This could be requests.post with a payload if the API needs POST methods rather than GET
    # response = requests.post(url=rest_endpoint, data=api_params)

    # Check for a valid response, if not return no rows
    # You could instead through an exception here (https://www.postgresql.org/docs/10/plpython-util.html)
    if response.status_code >= 400:
        raise plpy.Error('REST API request no work')
        return []
    # Grab the JSON from the response (if your API is returning plain-text or XML, use response.text)
    rest_json = response.json()

    #
    # Transform Object response into Rows and Columns and Insert the data into the Table
    #

    # Per https://www.postgresql.org/docs/10/plpython-database.html#id-1.8.11.15.3
    # When inserting,  you need to create a Query Plan first, with placeholders for values
    # and a list of PostgreSQL types
    insert_query = "INSERT INTO {} VALUES(NOW(), $1, $2, $3, $4, $5, $6, $7, $8)".format(storage_table_name)
    # First type will always be text for the unique request id, but other types are up to you
    insert_types = ["text", "date", "real", "real", "real", "real", "real", "real"]
    # We prepare the query, which we will use over and over for each row to insert
    # this might be better implemented with explicit subtransactions https://www.postgresql.org/docs/10/plpython-subtransaction.html
    plan = plpy.prepare(insert_query, insert_types)

    #
    # Working through the JSON response to build the list to insert into the database
    #

    # Most JSON requests have an initial key (like {'response' : etc. } ) as first-level
    # Very rarely do they consist of a directly returned array, which would not need this first for-loop
    for first_level in rest_json:
        data_in_first_level = rest_json[first_level]
        # In this example, there are many keys to pick from, but the 'data' key
        # returns a list of lists, each representing a one-dimensional row of data
        for second_level in data_in_first_level['data']:
            # You might have additional transformations to do here, if some values are
            # arrays or objects, or have additional levels of depth
            # Any logic to map from Object-to-Relational would happen here

            # Add the unique request id and any other attributes in columns that do not derive from the response
            # The timestamp is added automatically in the prepared query, so you don't need it here
            insert_list = [unique_request_id, ]
            # Now extend the list with the row of data from the API response
            insert_list.extend(second_level)
            plpy.execute(plan, insert_list)

# Regardless of whether you brought in new data or not, return the results of the query
# bound by the arguments. In this case, the dates are filtered in the API, rather than the query
return_query = """
SELECT * FROM 
{} 
WHERE unique_request_id = '{}'
""".format(storage_table_name, unique_request_id)

return plpy.execute(return_query)

$$ LANGUAGE plpython3u;