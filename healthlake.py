from datetime import datetime, date, timedelta
from io import StringIO

import arrow
import conf
import flask
import boto3
import json
import time
import csv


# initialize our app and our S3 and Athena clients
app = flask.Flask(__name__)
s3 = boto3.client('s3')
athena = boto3.client('athena')


#
# Utility Functions
#

def store(rows):
    '''
    Store rows of health export data in our S3 bucket.
    '''

    key_name = 'syncs/' + datetime.utcnow().isoformat() + '.json'

    # athena and glue prefer a row of JSON per line
    json_rows = [json.dumps(row).strip() for row in rows]
    content = '\n'.join(json_rows)

    s3.put_object(
        Bucket=conf.bucket,
        Key=key_name,
        Body=content
    )


def transform(data):
    '''
    Flatten the nested JSON data structure from Health Export
    in order to make it easier to index and query with Athena.
    '''

    rows = []
    for metric in data.get('data', {}).get('metrics', []):
        name = metric['name']
        units = metric['units']

        for point in metric.get('data', []):
            point['name'] = name
            point['units'] = units
            rows.append(point)

    return rows


def wait_on_query(query, output):
    '''
    Send a query to Athena, specify the output location for the
    results of the query, and then wait on the query to execute
    before returning the results.
    '''

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': conf.database
        },
        ResultConfiguration={
            'OutputLocation': output
        }
    )

    execution_id = response['QueryExecutionId']

    # wait on the query to complete... Lambda timeout is the ceiling (30s)
    while True:
        time.sleep(1)

        result = athena.get_query_execution(QueryExecutionId=execution_id)
        state = result['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            return result


def generate_detail(date):
    '''
    Store a single, JSON-formatted object containing all results
    and data points for a single, specified date, in the format
    `YYYY-MM-DD`.
    '''

    # query athena for all data points for the specified date
    query = '''
        SELECT * FROM history WHERE
            datetime >= TIMESTAMP '%(date)s 00:00:00'
        AND
            datetime <= TIMESTAMP '%(date)s 23:59:59'
    ''' % dict(date=date)

    output = 's3://' + conf.bucket + '/summaries/' + date

    result = wait_on_query(query, output)

    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='summaries/' + date
    )
    if 'Contents' not in response:
        raise Exception('Query failed')

    # find the CSV object containing the query results
    for key in response['Contents']:
        if key['Key'].endswith('.csv'):
            response = s3.get_object(
                Bucket=conf.bucket,
                Key=key['Key']
            )

            # parse the CSV data
            reader = csv.DictReader(
                StringIO(response['Body'].read().decode('utf-8'))
            )

            # generate our data rollup
            result = {}
            for line in reader:
                result[line['name']] = {
                    key: value
                    for (key, value) in line.items()
                    if value and key not in ('name', 'date')
                }

            # store the data rollup in S3
            s3.put_object(
                Bucket=conf.bucket,
                Key='summaries/' + date + '/results.json',
                Body=json.dumps(result)
            )

            return json.dumps(result)


def clear_cache(date):
    '''
    '''
    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='summaries/' + date
    )
    if 'Contents' not in response:
        raise Exception('Query failed')

    for key in response['Contents']:
        response = s3.delete_object(
            Bucket=conf.bucket,
            Key=key['Key']
        )

    s3.delete_object(
        Bucket=conf.bucket,
        Key='summaries/' + date + '/results.json'
    )


def fetch_detail(date):
    '''
    Fetch the JSON summary of all results for a single date,
    specified as a string in the format 'YYYY-MM-DD'.
    '''

    # try and get the cached summary for the day
    try:
        response = s3.get_object(
            Bucket=conf.bucket,
            Key='summaries/' + date + '/results.json'
        )
        result = json.loads(response['Body'].read())

        # If the summary was generated less than 36 hours from
        # the targe date, but not in the last hour, then regenerate
        # to ensure our summary has had time for all of the data to
        # be synced to the data lake
        target_eod = arrow.get(date + ' 23:59:59')
        update_date = response['LastModified']
        now = arrow.now(tz=conf.tz)
        if update_date - target_eod < timedelta(hours=36):
            print('Result is less than 36 hours from target date')
            if now - update_date < timedelta(hours=1):
                print('Result in the last hour, returning.')
                print('\t', update_date)
                print('\t', now - update_date)
                return result

            print('Regenerating...')
            clear_cache(date)
            result = json.loads(generate_detail(date))

    # if we can't find it, generate it
    except s3.exceptions.NoSuchKey:
        result = json.loads(generate_detail(date))

    # return our summary
    return result


#
# HTTP Routes
#

@app.route('/sync', methods=['POST'])
def sync():
    '''
    Sync results from Health Export into our data lake.
    '''

    raw_data = flask.request.json
    transformed = transform(raw_data)
    store(transformed)

    return flask.jsonify(
        success=True,
        message='Successfully received and stored sync data.'
    )


@app.route('/detail/<datestr>', methods=['GET'])
def detail(datestr):
    '''
    Get summary of all results for a single date.
    '''

    # validate that the requested date is in the past
    today = arrow.now(tz=conf.tz).date()
    year, month, day = datestr.split('-')
    requested = date(int(year), int(month), int(day))

    if requested >= today:
        flask.abort(404, 'Please specify a date in the past.')

    # otherwise, fetch the detail and return it to the client
    data = fetch_detail(datestr)

    # if there is no data, then abort
    if not len(data):
        flask.abort(404, 'No data available for this date.')

    return flask.jsonify(data)
