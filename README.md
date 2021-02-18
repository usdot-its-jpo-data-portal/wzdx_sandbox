# wzdx_sandbox
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=usdot-its-jpo-data-portal_datahub-metrics-ingest&metric=alert_status)](https://sonarcloud.io/dashboard?id=usdot-its-jpo-data-portal_datahub-metrics-ingest)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=usdot-its-jpo-data-portal_datahub-metrics-ingest&metric=coverage)](https://sonarcloud.io/dashboard?id=usdot-its-jpo-data-portal_datahub-metrics-ingest)


This repository includes code for ingesting [Work Zone Data Exchange (WZDx)](https://github.com/usdot-jpo-ode/jpo-wzdx) feed data into ITS DataHub's Work Zone Data Archives. Specifically, this includes:
- Lambda function `wzdx_ingest_to_archive`. Triggered by `wzdx_trigger_ingest` lambda function from [wzdx_registry](https://github.com/usdot-its-jpo-data-portal/wzdx_registry) GitHub repository, this lambda uses the feed metadata passed from `wzdx_trigger_ingest` from [WZDx Feed Registry](https://datahub.transportation.gov/d/69qe-yiui) to ingest a raw copy of a WZDx feed into the [raw data sandbox](http://usdot-its-workzone-raw-public-data.s3.amazonaws.com/index.html) in the Work Zone Data Archive and triggers `wzdx_ingest_to_lake` and `wzdx_ingest_to_socrata` lambda functions based on the feed's metadata.
- Lambda function `wzdx_ingest_to_lake`. This lambda function ingests a semi-processed copy of a WZDx feed into the [semi-processed data sandbox](http://usdot-its-workzone-public-data.s3.amazonaws.com/index.html) in the Work Zone Data Archive.
- Lambda function `wzdx_ingest_to_socrata`. This lambda function ingests a transformed tabular copy of a WZDx feed into a integrated Socrata dataset on data.transportation.gov that is associated with the feed.
- Landing page for the Work Zone Data Archive S3 explorer sites.

For more information on ITS Sandbox data, please refer to the [ITS Sandbox README page](https://github.com/usdot-its-jpo-data-portal/sandbox). For an overview on WZDx resources on ITS DataHub, please refer to the [WZDx data story](https://datahub.transportation.gov/d/jixs-h7uw).

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

#### Prerequisites for AWS Lambda Deployment

If you plan to deploy the script on AWS Lambda, you need access to an AWS account and be able to assign role(s) to a lambda function. There needs to be a role that is able to invoke lambda functions and perform list/read/write actions to relevant buckets in S3.

#### Prerequisites for Local Deployment

If you plan to deploy the script on your local machine, you need the following:

1. Have access to Python 3.6+. You can check your python version by entering `python --version` and `python3 --version` in command line.
2. Have access to the command line of a machine. If you're using a Mac, the command line can be accessed via the [Terminal](https://support.apple.com/guide/terminal/welcome/mac), which comes with Mac OS. If you're using a PC, the command line can be accessed via the Command Prompt, which comes with Windows, or via [Cygwin64](https://www.cygwin.com/), a suite of open source tools that allow you to run something similar to Linux on Windows.
3. Have your own Free Amazon Web Services account.
	- Create one at http://aws.amazon.com
4.  Obtain Access Keys:
	- On your Amazon account, go to your profile (at the top right)
	- My Security Credentials > Access Keys > Create New Access Key
	- Record the Access Key ID and Secret Access Key ID (you will need them in step 4)
5. Save your AWS credentials in your local machine, using one of the following method:
	- shared credentials file: instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file.
	- environmental variables: instructions at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#environment-variables

### Installing locally

1. Download the script by cloning the git repository at https://github.com/usdot-its-jpo-data-portal/wzdx_sandbox. You can do so by running the following in command line.
`git clone https://github.com/usdot-its-jpo-data-portal/wzdx_sandbox.git`. If unfamiliar with how to clone a repository, follow the guide at https://help.github.com/en/articles/cloning-a-repository.
2. Navigate into the repository folder by entering `cd wzdx_sandbox` in command line.
3. Install the required packages by running `pip install -r lambda__wzdx_ingest_to_lake__requirements.txt` and `pip install -r lambda__wzdx_ingest_to_socrata__requirements.txt`.


## Deployment

### Deployment on AWS Lambda

1. To prepare the code package for deployment to AWS Lambda, run `sh package.sh` to build the packages. This will create two files in the repo's root folder: `wzdx_ingest_to_archive.zip`, `wzdx_ingest_to_lake.zip`, and `wzdx_ingest_to_socrata.zip`.
2. For each of the lambdas, create a lambda function in your AWS account "from scratch" with the following setting:
	- Runtime: Python 3.8
	- Permissions: Use an existing role (choose existing role with full lambda access (e.g. policy AWSLambdaFullAccess) and list/read/write permission to your destination s3 bucket)
3. In the configuration view of your lambda function, set the following:
	- For the `wzdx_ingest_to_archive` function:
		- In "Function code" section, select "Upload a .zip file" and upload the `wzdx_ingest_to_archive.zip` file as your "Function Package."
		- In "Environment variables" section, set the following:
		  - `BUCKET`: the destination s3 bucket where the WZDx feed should be archived to.
			  - default set as: usdot-its-workzone-raw-public-data
      - `LAMBDA_TO_TRIGGER`: the name of the lambda for the `wzdx_ingest_to_lake` function or some other lambda that this function should trigger.
		    - default set as: wzdx_ingest_to_lake
			- `SOCRATA_LAMBDA_TO_TRIGGER`: the name of the lambda for the `wzdx_ingest_to_socrata` function or some other lambda that this function should trigger.
		    - default set as: wzdx_ingest_to_socrata
		- In "Basics settings" section, set adequate Memory and Timeout values. Memory of 1664 MB and Timeout value of 10 minutes should be plenty.
	- For the `wzdx_ingest_to_lake` function:
		- In "Function code" section, select "Upload a .zip file" and upload the `wzdx_ingest_to_lake.zip` file as your "Function Package."
		- In "Environment variables" section, set the following:
			- `BUCKET`: the destination s3 bucket where the WZDx feed should be archived to.
				- default set as: usdot-its-workzone-public-data
		- In "Basics settings" section, set adequate Memory and Timeout values. Memory of 1664 MB and Timeout value of 10 minutes should be plenty.
	- For the `wzdx_ingest_to_socrata` function:
		- In "Function code" section, select "Upload a .zip file" and upload the `wzdx_ingest_to_socrata.zip` file as your "Function Package."
		- In "Environment variables" section, set the following:
	    - `SOCRATA_PARAMS`: stringified json object containing Socrata credentials for a user that has write access to the WZDx feed registry. At a minimum, this should include `username`, `password`, `app_token`, and `domain`. If you do not have a `app_token` you can set it as an empty string.
		- In "Basics settings" section, set adequate Memory and Timeout values. Memory of 1664 MB and Timeout value of 10 minutes should be plenty.
4. Make sure to save all of your changes.

### Invocation of the Lambdas

All three lambda functions expect to be invoked via code.

In our deployment, the `wzdx_ingest_to_archive` is invoked by a scheduled `wzdx_trigger_ingest` lambda that is described in the [wzdx_registry](https://github.com/usdot-its-jpo-data-portal/wzdx_registry) GitHub repository. Both `wzdx_ingest_to_lake` lambda and `wzdx_ingest_to_socrata` lambda are invoked by the `wzdx_ingest_to_archive` lambda, depending on a feed's metadata.

The lambdas to be invoked expect the following information in the payload:

* For the `wzdx_ingest_to_archive` function, the payload should be sent as a stringified dict object with the following fields:
  * `feed`: the row dictionary object for a particular feed in the [WZDx Feed Registry](https://datahub.transportation.gov/d/69qe-yiui).
  * `dataset_id`: the dataset id of the [WZDx Feed Registry](https://datahub.transportation.gov/d/69qe-yiui).
* For the `wzdx_ingest_to_lake` function, the payload should be sent as a stringified dict object with the following fields:
  * `feed`: the row dictionary object for a particular feed in the [WZDx Feed Registry](https://datahub.transportation.gov/d/69qe-yiui).
  * `bucket`: the name of the S3 bucket that contains the feed snapshot to be parsed
  * `key`: the prefix of the S3 bucket path that contains the feed snapshot to be parsed
* For the `wzdx_ingest_to_socrata` function, the payload should be sent as a stringified dict object with the following fields:
  * `feed`: the row dictionary object for a particular feed in the [WZDx Feed Registry](https://datahub.transportation.gov/d/69qe-yiui).
  * `bucket`: the name of the S3 bucket that contains the feed snapshot to be parsed
  * `key`: the prefix of the S3 bucket path that contains the feed snapshot to be parsed

### Deployment of S3 Explorer site

1. Upload `index.html` to the root folder of your S3 bucket.
2. In the AWS Console for your S3 bucket, go to "Permissions" > "CORS configuration" and copy and paste the following block of text and replace `{YOUR_WORKZONE_BUCKET_NAME}` with your bucket name.

```
<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<CORSRule>
    <AllowedOrigin>*</AllowedOrigin>
    <AllowedOrigin>http://{YOUR_WORKZONE_BUCKET_NAME}.s3.amazonaws.com</AllowedOrigin>
    <AllowedOrigin>https://s3.amazonaws.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>HEAD</AllowedMethod>
    <MaxAgeSeconds>3000</MaxAgeSeconds>
    <ExposeHeader>ETag</ExposeHeader>
    <ExposeHeader>x-amz-meta-custom-header</ExposeHeader>
    <AllowedHeader>Authorization</AllowedHeader>
    <AllowedHeader>*</AllowedHeader>
</CORSRule>
</CORSConfiguration>
```

3. Save. Also make sure that your bucket policy allows for List/Get actions on resource `arn:aws:s3:::{YOUR_WORKZONE_BUCKET_NAME}/*` and `arn:aws:s3:::{YOUR_WORKZONE_BUCKET_NAME}`.


## Built With

* [Python 3.6+](https://www.python.org/download/releases/3.0)
* [requests](https://pypi.org/project/requests/): package managing HTTP requests.
* [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html?id=docs_gateway): AWS API.
* [xmltodict](https://github.com/martinblech/xmltodict) : "Python module that makes working with XML feel like you are working with JSON."
* [sodapy](https://github.com/xmunoz/sodapy): Python client for the Socrata Open Data API.
* [ITS DataHub sandbox exporter](https://github.com/usdot-its-jpo-data-portal/sandbox_exporter): Package to load, query, and export data from ITS DataHub's sandbox efficiently.

## Contributing

1. [Fork it](https://github.com/usdot-its-jpo-data-portal/wzdx_sandbox/fork)
2. Create your feature branch (git checkout -b feature/fooBar)
3. Commit your changes (git commit -am 'Add some fooBar')
4. Push to the branch (git push origin feature/fooBar)
5. Create a new Pull Request

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for general good practices on code of conduct, and the process for submitting pull requests.

## License

This project is licensed under the Apache 2.0 License. - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* Thank you to the Department of Transportation for funding to develop this project.

## Code.gov Registration Info

Agency: DOT

Short Description: Code for the Work Zone Data Exchange feed ingestion pipeline.

Status: Beta

Tags: transportation, connected vehicles, intelligent transportation systems, python, ITS Sandbox, Socrata, work zone data exchange (WZDx), smart work zone

Labor hours: 0

Contact Name: Brian Brotsos

Contact Phone: (202) 366-9013
