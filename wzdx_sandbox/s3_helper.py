"""
AWS and AWS S3 Helper functions.

"""
import boto3
import botocore.exceptions
import json
import logging
import traceback
import inspect


class aws_helper(object):
    """
    Helper class for connecting to AWS.

    """
    def __init__(self, aws_profile=None, logger=False):
        """
        Initialization function of the aws_helper class.

        Parameters:
            aws_profile: Optional string name of your AWS profile, as set up in
                the credential file at ~/.aws/credentials. No need to pass in
                this parameter if you will be using your default profile. For
                additional information on how to set up the credential file, see
                https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
            logger: Optional parameter. Could pass in a logger object or not pass
                in anything. If a logger object is passed in, information will be
                logged instead of printed. If not, information will be printed.
        """
        self.aws_profile = aws_profile
        self.print_func = print
        if logger:
            self.print_func = logger.info
        self.session = self._create_aws_session()

    def _create_aws_session(self):
        """
        Creates AWS session using aws profile name passed in or using aws
        credentials in environment variables.

        Returns:
            AWS session object.
        """
        try:
            if self.aws_profile:
                session = boto3.session.Session(profile_name=self.aws_profile)
            else:
                session = boto3.session.Session()
        except botocore.exceptions.ProfileNotFound:
            self.print_func('Please supply a valid AWS profile name.')
            exit()
        except:
            self.print_func(traceback.format_exc())
            self.print_func('Exiting. Unable to establish AWS session with the following profile name: {}'.format(self.aws_profile))
            raise
        return session


class S3Helper(aws_helper):
    """
    Helper class for connecting to and working with AWS S3.

    """
    def __init__(self, **kwargs):
        """
        Initialization function of the S3Helper class.

        """
        super(S3Helper, self).__init__(**kwargs)
        self.client = self._get_client()

    def _get_client(self):
        """
        Creates S3 client.

        Returns:
            AWS S3 client.
        """
        return self.session.client('s3')

    def path_exists(self, bucket, path):
        """
        Check if S3 path exists.

        Parameters:
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            Boolean (True/False)
        """
        try:
            self.client.head_object(Bucket=bucket, Key=path)
            return True
        except self.client.exceptions.NoSuchKey:
            self.print_func("NoSuchKey error caught, path does not exist.")
            return False
        except botocore.exceptions.ClientError:
            self.print_func("ClientError caught, assuming path does not exist.")
            return False

    def get_data_stream(self, bucket, key):
        """
        Get data stream.

        Parameters:
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            "Readable" file datastream objects
        """
        obj = self.client.get_object(Bucket=bucket, Key=key)
        if key[-3:] == '.gz':
            gzipped = GzipFile(None, 'rb', fileobj=obj['Body'])
            data = TextIOWrapper(gzipped)
        else:
            data = obj['Body']
        return data

    def newline_json_rec_generator(self, data_stream):
        """
        Receives a data stream that is assumed to be in the newline JSON format
        (one stringified json per line), reads and returns these records as
        dictionary objects one at a time.

        Parameters:
            data_stream: "Readable" file datastream objects

        Returns:
            Iterable array of dictionary objects
        """
        line = data_stream.readline()
        while line:
            if type(line) == bytes:
                line_stripped = line.strip(b'\n')
            else:
                line_stripped = line.strip('\n')

            try:
                if line_stripped:
                    yield json.loads(line_stripped)
            except:
                self.print_func(traceback.format_exc())
                self.print_func('Invalid json line. Skipping: {}'.format(line))
                self.err_lines.append(line)
                raise
            line = data_stream.readline()

    def write_recs(self, recs, bucket, key):
        """
        Writes the array of dictionary objects as newline json text file to the
        specified S3 key in the specified S3 bucket

        Parameters:
            recs: array of dictionary objects
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            None
        """
        json_list = []
        for i in recs:
            if i is not None and not inspect.isfunction(i):
                json_list.append(json.dumps(i))
        outbytes = "\n".join(json_list).encode('utf-8')
        self.client.put_object(Bucket=bucket, Key=key, Body=outbytes)

    def write_bytes(self, outbytes, bucket, key):
        """
        Writes the bytes to the specified S3 key in the specified S3 bucket

        Parameters:
            outbytes: bytes
            bucket: name of S3 bucket
            path: key of S3 path

        Returns:
            None
        """
        if type(outbytes) != bytes:
            outbytes = outbytes.encode('utf-8')
        self.client.put_object(Bucket=bucket, Key=key, Body=outbytes)
