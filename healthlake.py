from datetime import datetime, date, timedelta
from io import StringIO

import arrow
import conf
import flask
import boto3
import json
import time
import csv
import sys


# initialize our app and our S3 and Athena clients
app = flask.Flask(__name__)
s3 = boto3.client('s3')
athena = boto3.client('athena')

# force the ability to parse very large CSV files
csv.field_size_limit(sys.maxsize)


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


def store_workouts(workouts):
    '''
    Store rows of workout data in our S3 bucket.
    '''

    key_name = 'workouts/' + datetime.utcnow().isoformat() + '.json'

    # athena and glue prefer a row of JSON per line
    json_rows = [json.dumps(workout).strip() for workout in workouts]
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


def transform_workouts(data):
    '''
    Flatten the nested JSON data structure from Health Export
    for workouts to make it easier to index and query with Athena.
    '''

    workouts = []
    for raw in data.get('data', {}).get('workouts', []):
        workout = {}
        for key, val in raw.items():
            if isinstance(val, dict):
                for subkey, subval in val.items():
                    workout['_'.join([key, subkey])] = subval
            else:
                workout[key] = val
        workouts.append(workout)

    return workouts


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


def generate_workout_detail(date):
    '''
    Store a single, JSON-formatted object containing all results
    and data points for a single, specified date, in the format
    `YYYY-MM-DD`.
    '''

    # query athena for all data points for the specified date
    query = '''
        SELECT * FROM workout_history WHERE
            start_datetime >= TIMESTAMP '%(date)s 00:00:00'
        AND
            start_datetime <= TIMESTAMP '%(date)s 23:59:59'
    ''' % dict(date=date)

    output = 's3://' + conf.bucket + '/workout-summaries/' + date

    result = wait_on_query(query, output)

    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='workout-summaries/' + date
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
            results = []
            for line in reader:
                result = {
                    key: value
                    for (key, value) in line.items()
                    if key not in ('route', 'route_json')
                }
                if 'route_json' in line:
                    result['route'] = json.loads(line['route_json'])

                results.append(result)

            # store the data rollup in S3
            s3.put_object(
                Bucket=conf.bucket,
                Key='workout-summaries/' + date + '/results.json',
                Body=json.dumps(result)
            )

            return json.dumps(result)


def generate_summary(date):
    '''
    Store a single, JSON-formatted object containing all results
    and data points for a single, specified month, in the format
    `YYYY-MM`.
    '''

    # calculate date range
    start = arrow.get(date).floor('month')
    end = start.ceil('month')

    # query athena for all data points for the specified date
    query = '''
        SELECT * FROM history WHERE
            datetime >= TIMESTAMP '%(start)s 00:00:00'
        AND
            datetime <= TIMESTAMP '%(end)s 23:59:59'
    ''' % dict(
        start=start.date().isoformat(),
        end=end.date().isoformat()
    )

    output = 's3://' + conf.bucket + '/monthly-summaries/' + date

    result = wait_on_query(query, output)

    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='monthly-summaries/' + date
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
                line_day = arrow.get(line['date'][:10]).date().isoformat()
                day_results = result.setdefault(line_day, {})

                day_results[line['name']] = {
                    key: value
                    for (key, value) in line.items()
                    if value and key not in ('name', 'date')
                }

            # store the data rollup in S3
            s3.put_object(
                Bucket=conf.bucket,
                Key='monthly-summaries/' + date + '/results.json',
                Body=json.dumps(result)
            )

            return json.dumps(result)


def generate_global_metrics():
    '''
    Fetch the JSON global metrics summary for all health
    data stored in the data lake.
    '''

    # query athena for all data points for the specified date
    query = 'SELECT * FROM daily_global_metrics'

    output = 's3://' + conf.bucket + '/global-metrics'

    result = wait_on_query(query, output)

    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='global-metrics'
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
                result = dict(line.items())

            # store the data rollup in S3
            s3.put_object(
                Bucket=conf.bucket,
                Key='global-metrics/results.json',
                Body=json.dumps(result)
            )

            return json.dumps(result)


def clear_cache(date):
    '''
    For the specified date, destroy all cached queries and daily rollups.
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


def clear_workout_cache(date):
    '''
    For the specified date, destroy all of the cached queries and daily rollups
    '''

    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='workout-summaries/' + date
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
        Key='workout-summaries/' + date + '/results.json'
    )


def clear_global_cache():
    '''
    Destroy global metrics cache.
    '''

    # find the CSV results at the specified output path in S3
    response = s3.list_objects_v2(
        Bucket=conf.bucket,
        Prefix='global-metrics/'
    )
    if 'Contents' not in response:
        raise Exception('Query failed')

    for key in response['Contents']:
        response = s3.delete_object(
            Bucket=conf.bucket,
            Key=key['Key']
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
            if now - update_date < timedelta(hours=1):
                return result

            clear_cache(date)
            result = json.loads(generate_detail(date))

    # if we can't find it, generate it
    except s3.exceptions.NoSuchKey:
        result = json.loads(generate_detail(date))

    # return our summary
    return result


def fetch_summary(date):
    '''
    Fetch the JSON summary of all results for a single month,
    specified as a string in the format 'YYYY-MM'.
    '''

    # try and get the cached summary for the month
    try:
        response = s3.get_object(
            Bucket=conf.bucket,
            Key='monthly-summaries/' + date + '/results.json'
        )
        result = json.loads(response['Body'].read())

    # if we can't find it, generate it
    except s3.exceptions.NoSuchKey:
        result = json.loads(generate_summary(date))

    # return our summary
    return result


def fetch_global_metrics():
    '''
    Fetch the JSON global metrics summary for all health
    data stored in the data lake.
    '''

    # try and get the cached summary
    try:
        response = s3.get_object(
            Bucket=conf.bucket,
            Key='global-metrics/results.json'
        )
        result = json.loads(response['Body'].read())

        # If the summary was generated more than 24 hours ago,
        # regenerate to ensure we have the latest metrics
        update_date = response['LastModified']
        now = arrow.now(tz=conf.tz)
        if now - update_date > timedelta(hours=24):
            if now - update_date > timedelta(hours=1):
                return result

            clear_global_cache()
            result = json.loads(generate_global_metrics())

    # if we can't find it, generate it
    except s3.exceptions.NoSuchKey:
        result = json.loads(generate_global_metrics())

    return result


def fetch_workouts(date):
    # try and get the cached workout summary for the day
    try:
        response = s3.get_object(
            Bucket=conf.bucket,
            Key='workout-summaries/' + date + '/results.json'
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
            if now - update_date < timedelta(hours=1):
                return result

            clear_workout_cache(date)
            result = json.loads(generate_workout_detail(date))

    # if we can't find it, generate it
    except s3.exceptions.NoSuchKey:
        result = json.loads(generate_workout_detail(date))

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

    # fetch the raw JSON data
    raw_data = flask.request.json

    # transform the sync data and store it
    transformed = transform(raw_data)
    store(transformed)

    # transform the workout data and store it
    workouts = transform_workouts(raw_data)
    store_workouts(workouts)

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


@app.route('/workouts/<datestr>', methods=['GET'])
def workouts(datestr):
    '''
    Get all workouts for a single date.
    '''

    # validate that the requested date is in the past
    today = arrow.now(tz=conf.tz).date()
    year, month, day = datestr.split('-')
    requested = date(int(year), int(month), int(day))

    if requested >= today:
        flask.abort(404, 'Please specify a date in the past.')

    # otherwise, fetch the detail and return it to the client
    data = fetch_workouts(datestr)

    # if there is no data, then abort
    if not len(data):
        flask.abort(404, 'No data available for this date.')

    return flask.jsonify(data)


@app.route('/summary/<monthstr>', methods=['GET'])
def summary(monthstr):
    '''
    Get summary of all results for a single month.
    '''

    # validate that the requested month is in the past
    month_start = arrow.now(tz=conf.tz).floor('month')
    requested = arrow.get(monthstr).replace(tzinfo=conf.tz)

    if requested >= month_start:
        flask.abort(404, 'Please request a month in the past.')

    # otherwise, fetch the summary and return it to the client
    data = fetch_summary(monthstr)

    # if there is no data, then abort
    if not len(data):
        flask.abort(404, 'No data available for this month.')

    return flask.jsonify(data)


@app.route('/global', methods=['GET'])
def global_metrics():
    '''
    Get some high level status for all of my health history.
    '''

    data = fetch_global_metrics()

    # if there is no data, then abort
    if not len(data):
        flask.abort(404, 'No data available.')

    return flask.jsonify(data)
