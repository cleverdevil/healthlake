Health Lake
===========

Health Lake is a work-in-progress web service that makes it possible to ingest
Apple HealthKit data into an Amazon S3 bucket, ready to be used as a queryable
data lake. The web service is dependent on an iOS app in the Apple App Store
called [Health Auto Export](https://apps.apple.com/us/app/health-auto-export-json-csv/id1115567069),
so you'll need to download, install, and configure it to use this service.

It is already in use by me for [my website](https://cleverdevil.io/health), but
there are some bits and pieces that are configured in my AWS environment,
including setting up tables and views in AWS Glue that enable the service to
query the data set using Athena. If there is enough interest, I may put together
some IaC templates (CloudFormation or Terraform) that can be used to do a more
complete deployment.

Feel free to look at the code, ask questions, and provide feedback!

On Usage
--------

As mentioned above, this is a work-in-progress, and some bits and pieces for the
complete project aren't included, but here is a quick summary of how I
use/deploy the web service.

Install the [serverless framework](https://serverless.com) and then copy the
sample configuration files to "real" ones. Notably, you'll need to have a file
called `conf.py` for configuring the web service and `serverless.yml` which is
used by the serverless framework to do the full deployment of the service to
Lambda with the appropriate API Gateway configuration and IAM settings to ensure
the function has access to S3, Athena, and Glue.

Once the serverless framework is installed and you've made the appropriate
changes to `serverless.yml` and `conf.py`, you need to create a Python virtual
environment, activate it, and install requirements:

```
$ virtualenv -p python3.6 venv
$ . venv/bin/activate
$ pip install -r requirements.txt
```

At this point, you need to install a few serverless framework plugins:

```
$ sls plugin install -n serverless-wsgi
$ sls plugin install -n serverless-python-requirements
```

Now, you can test locally with `sls wsgi serve` or deploy to AWS with `sls
deploy`.

Web Service API 
---------------

There are only three routes for the API:

### `HTTP POST /sync`

This is the endpoint that you'll configure in the iOS app. I recommend
configuring the app to post once a day with daily granularity. I am not sure if
the service will work with other configurations.

Once the data is posted to this endpoint, the service will flatten the JSON data
structure and transform it to make it easy to leverage in AWS Glue and Athena.

### `HTTP GET /detail/<YYYY-MM-DD>`

This endpoint will return all metrics, in JSON format, that are available on the
specified day. There is some intelligence in the service for generating rollups
automatically, on-demand, to reduce cost and improve performance. If provided
with a date where data is not yet available, or is in the future, the service
will return a 404 with a descriptive status message.

### `HTTP GET /summary/<YYYY-MM>`

This endpoint will return all metrics, in JSON format, that are available on the
specified month. There is some intelligence in the service for generating rollups
automatically, on-demand, to reduce cost and improve performance. If provided
with a date where data is not yet available, or is in the future, the service
will return a 404 with a descriptive status message.
