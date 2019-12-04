import boto3
import botocore.exceptions
import json
import logging
import traceback


class AWS_helper(object):
    def __init__(self, aws_profile=None, logger=False):
        self.aws_profile = aws_profile
        self.print_func = print
        if logger:
            self.print_func = logger.info
        self.session = self._create_aws_session()

    def _create_aws_session(self):
        try:
            if self.aws_profile:
                session = boto3.session.Session(profile_name=self.aws_profile, region_name='us-east-1')
            else:
                session = boto3.session.Session()
        except botocore.exceptions.ProfileNotFound:
            self.print_func('Please supply a valid AWS profile name.')
            exit()
        except:
            self.print_func(traceback.format_exc())
            self.print_func('Exiting. Unable to establish AWS session with the following profile name: {}'.format(self.aws_profile))
            exit()
        return session


class S3Helper(AWS_helper):

    def __init__(self, **kwargs):
        super(S3Helper, self).__init__(**kwargs)
        self.client = self._get_client()

    def _get_client(self):
        return self.session.client('s3')

    def path_exists(self, bucket, path):
        try:
            self.client.get_object(Bucket=bucket, Key=path)
            return True
        except self.client.exceptions.NoSuchKey:
            return False

    def get_data_stream(self, bucket, key):
        obj = self.client.get_object(Bucket=bucket, Key=key)
        if key[-3:] == '.gz':
            gzipped = GzipFile(None, 'rb', fileobj=obj['Body'])
            data = TextIOWrapper(gzipped)
        else:
            data = obj['Body']._raw_stream
        return data

    def newline_json_rec_generator(self, data_stream):
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
            line = data_stream.readline()

    def write_recs(self, recs, bucket, key):
        outbytes = "\n".join([json.dumps(i) for i in recs if i]).encode('utf-8')
        self.client.put_object(Bucket=bucket, Key=key, Body=outbytes)

    def write_bytes(self, outbytes, bucket, key):
        if type(outbytes) != bytes:
            outbytes = outbytes.encode('utf-8')
        self.client.put_object(Bucket=bucket, Key=key, Body=outbytes)
