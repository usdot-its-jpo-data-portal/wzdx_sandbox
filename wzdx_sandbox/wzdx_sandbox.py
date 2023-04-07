"""
Class for working with ITS Work Zone Sandboxes.

"""
from copy import deepcopy
from datetime import datetime, timedelta
import dateutil.parser
import json
import logging
import requests
import xmltodict
import traceback
import csv

from wzdx_sandbox.s3_helper import S3Helper

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging


class ITSSandbox(object):
    """
    Base class for working with ITS Sandbox.

    """
    def __init__(self, bucket, aws_profile=None, logger=None):
        """
        Initialization function of the ITSSandbox class.

        Parameters:
            bucket: Name of the AWS S3 bucket that contains the ITS Sandbox.
            aws_profile: Optional string name of your AWS profile, as set up in
                the credential file at ~/.aws/credentials. No need to pass in
                this parameter if you will be using your default profile. For
                additional information on how to set up the credential file, see
                https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
            logger: Optional parameter. Could pass in a logger object or not pass
                in anything. If a logger object is passed in, information will be
                logged instead of printed. If not, information will be printed.
        """
        self.bucket = bucket
        self.s3helper = S3Helper(aws_profile=aws_profile)
        self.print_func = print
        if logger:
            self.print_func = logger.info

    def generate_fp(self, template, **kwargs):
        return template.format(**kwargs)


class WorkZoneRawSandbox(ITSSandbox):
    """
    Class for working with ITS Work Zone Raw Sandbox.

    """
    def __init__(self, bucket, feed=None, lambda_to_trigger=None,
                socrata_lambda_to_trigger=None,
                **kwargs):
        """
        Initialization function of the WorkZoneRawSandbox class.

        Parameters:
            bucket: Name of the AWS S3 bucket that contains the ITS Work Zone Raw Sandbox.
            feed: Dictionary object. Should be a record read from the WZDx feed
                registry Socrata dataset, with all fields, including the system
                fields (e.g. ':id').
            lambda_to_trigger: Name of the feed ingestion-parsing lambda function you'd like
                to invoke.
            aws_profile: Optional string name of your AWS profile, as set up in
                the credential file at ~/.aws/credentials. No need to pass in
                this parameter if you will be using your default profile. For
                additional information on how to set up the credential file, see
                https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
            logger: Optional parameter. Could pass in a logger object or not pass
                in anything. If a logger object is passed in, information will be
                logged instead of printed. If not, information will be printed.
        """
        super(WorkZoneRawSandbox, self).__init__(bucket, **kwargs)
        self.prefix_template = 'state={state}/feedName={feedname}/year={year}/month={month}/'
        self.feed = feed
        self.lambda_to_trigger = lambda_to_trigger
        self.socrata_lambda_to_trigger = socrata_lambda_to_trigger
        self.url_dict = {}
        self.read_urls()
        # variables necessary to update last ingest time to Socrata WZDx feed registry
        # this is currently done in the previous lambda function "wzdx_trigger_ingest".
        # leaving the block below in case we move the step to this function

    def read_urls(self):
        header = True
        with open('WZDx_URLs.csv') as in_f:
            for line in in_f:
                if header:
                    header = False
                    continue
                row = line.strip('\n').split(',')
                self.url_dict[(row[0],row[1])] = row[2]

    def ingest(self):
        """
        Method to ingest the raw feed to the ITS Work Zone Raw Sandbox and trigger
        the lambda that will ingest and process the feed further to the ITS Work
        Zone Semi-processed Sandbox.

        """
        datetime_retrieved = datetime.now()
        prefix = self.prefix_template.format(**self.feed, year=datetime_retrieved.strftime('%Y'), month=datetime_retrieved.strftime('%m'))
        fp = self.generate_fp(
            template='{feedname}_{datetime_retrieved}',
            feedname=self.feed['feedname'],
            datetime_retrieved=datetime_retrieved
        )

        url_to_request = self.url_dict[(self.feed['state'],self.feed['feedname'])]
        try:
            r = requests.get(url_to_request)
            if r.status_code == 200:
                data_to_write = r.content
                self.s3helper.write_bytes(data_to_write, self.bucket, key=prefix+fp)
                self.print_func('Raw data ingested from {} to {} at {} UTC'.format(url_to_request, prefix+fp, datetime_retrieved))
            else:
                self.print_func('Received status code {} from {} feed.'.format(r.status_code,self.feed['feedname']))
                self.print_func('Skip triggering ingestion of {} to sandbox.'.format(self.feed['feedname']))
                self.print_func('Skip triggering ingestion of {} to Socrata.'.format(self.feed['feedname']))
                return
        except BaseException as e:
            data_to_write = f'The feed at {datetime_retrieved.isoformat()}.'.encode('utf-8')
            fp += '__FEED_NOT_RETRIEVED'
            self.s3helper.write_bytes(data_to_write, self.bucket, key=prefix+fp)
            self.print_func('We could not ingest data from {} at {} UTC'.format(url_to_request, datetime_retrieved))
            raise e

        # trigger semi-parse ingest
        if self.feed['pipedtosandbox'] == True:
            self.print_func('Trigger {} for {}'.format(self.lambda_to_trigger, self.feed['feedname']))
            lambda_client = self.s3helper.session.client('lambda')
            data_to_send = {'feed': self.feed, 'bucket': self.bucket, 'key': prefix+fp}
            response = lambda_client.invoke(
                FunctionName=self.lambda_to_trigger,
                InvocationType='Event',
                LogType='Tail',
                ClientContext='',
                Payload=json.dumps(data_to_send).encode('utf-8')
            )
            self.print_func(response)
        else:
            self.print_func('Skip triggering ingestion of {} to sandbox.'.format(self.feed['feedname']))

        # trigger ingest to socrata
        if self.feed['pipedtosocrata'] == True:
            self.print_func('Trigger {} for {}'.format(self.socrata_lambda_to_trigger, self.feed['feedname']))
            lambda_client = self.s3helper.session.client('lambda')
            data_to_send = {'feed': self.feed, 'bucket': self.bucket, 'key': prefix+fp}
            response = lambda_client.invoke(
                FunctionName=self.socrata_lambda_to_trigger,
                InvocationType='Event',
                LogType='Tail',
                ClientContext='',
                Payload=json.dumps(data_to_send).encode('utf-8')
            )
            self.print_func(response)
        else:
            self.print_func('Skip triggering ingestion of {} to Socrata.'.format(self.feed['feedname']))


class WorkZoneSandbox(ITSSandbox):
    """
    Class for working with ITS Work Zone Sandbox.

    """
    def __init__(self, bucket, feed=None, **kwargs):
        """
        Initialization function of the WorkZoneSandbox class.

        Parameters:
            bucket: Name of the AWS S3 bucket that contains the ITS Sandbox.
            feed: Dictionary object. Should be a record read from the WZDx feed
                registry Socrata dataset, with all fields, including the system
                fields (e.g. ':id').
            aws_profile: Optional string name of your AWS profile, as set up in
                the credential file at ~/.aws/credentials. No need to pass in
                this parameter if you will be using your default profile. For
                additional information on how to set up the credential file, see
                https://docs.aws.amazon.com/sdk-for-php/v3/developer-guide/guide_credentials_profiles.html
            logger: Optional parameter. Could pass in a logger object or not pass
                in anything. If a logger object is passed in, information will be
                logged instead of printed. If not, information will be printed.
        """
        super(WorkZoneSandbox, self).__init__(bucket, **kwargs)
        self.prefix_template = 'state={state}/feedName={feedname}/year={year}/month={month}/'
        self.feed = feed

        self.n_new_status = 0
        self.n_overwrite = 0
        self.n_new_fps = 0
        self.n_skipped = 0

    def ingest(self, data):
        """
        Method to ingest and parse the raw feed from the ITS Work Zone Raw Sandbox
        to the ITS Work Zone Semi-processed Sandbox.

        Parameters:
            data: Raw string data from feed archive sandbox. Could be stringified
                JSON object or XML.
        """
        self.print_func('Ingesting data from {} feed.'.format(self.feed['feedname']))
        data = self.parse_to_json(data)
        new_statuses, generate_out_rec, prefix, field_name_tuple = self.generate_fp_status_dict(data)

        for fp, current_status in new_statuses.items():
            key = prefix+fp
            out_rec = generate_out_rec(current_status)
            if self.s3helper.path_exists(self.bucket, key):
                out_recs = self.combine_with_existing_recs(key, out_rec, field_name_tuple)
                if out_recs is None:
                    continue
            else:
                out_recs = [out_rec]
                self.n_new_fps += 1
            self.s3helper.write_recs(out_recs, self.bucket, key)

        self.print_func('{} status found in {} feed: {} skipped, {} overwrites, {} updates, {} new files'.format(
        len(new_statuses), self.feed['feedname'], self.n_skipped, self.n_overwrite, self.n_new_status, self.n_new_fps))

    def parse_to_json(self, data):
        """
        Method to parse the string data received to json.

        Parameters:
            data: Raw string data from feed archive sandbox. Could be stringified
                JSON object or XML.
        Returns:
            Dictionary object equivalent of the data.
        """
        try:
            feed_format = self.feed['format']
            if type(data) == dict:
                out = data
            elif feed_format == 'xml':
                xmldict = xmltodict.parse(data)
                out = json.loads(json.dumps(xmldict))
            elif feed_format in ['json', 'geojson']:
                out = json.loads(data)
            else:
                out = data
            return out
        except BaseException as e:
            self.print_func('ERROR WITH FEED')
            self.print_func('FEED: {}'.format(self.feed))
            self.print_func('DATA: {}'.format(data))
            self.print_func(traceback.format_exc())
            raise e
            
    def generate_fp_status_dict(self, data):
        if self.feed['version'] == '1':
            # spec version 1
            data = data['WZDx']
            activity_list_field_name = 'WorkZoneActivity'
            header_field_name = 'Header'
            update_time_field_name = 'timeStampUpdate'
            feed_version = data[header_field_name]['versionNo']
            generate_out_rec = lambda activity: {header_field_name: data[header_field_name], activity_list_field_name: [activity]}
        elif self.feed['version'] == '2':
            # spec version 2
            activity_list_field_name = 'features'
            header_field_name = 'road_event_feed_info'
            update_time_field_name = 'feed_update_date'
            feed_version = data[header_field_name]['version']
            generate_out_rec = lambda activity: {header_field_name: data[header_field_name], activity_list_field_name: [activity], 'type': data['type']}
        elif '3' in self.feed['version']:
            # spec version 3, 3.1
            activity_list_field_name = 'features'
            header_field_name = 'road_event_feed_info'
            update_time_field_name = 'update_date'
            feed_version = data[header_field_name]['version']
            generate_out_rec = lambda activity: {header_field_name: data[header_field_name], activity_list_field_name: [activity], 'type': data['type']}
        else:
            #spec version 4, 4.1
            activity_list_field_name = 'features'
            if 'road_event_feed_info' in data:
                header_field_name = 'road_event_feed_info'
            else:
                header_field_name = 'feed_info'
            update_time_field_name = 'update_date'
            feed_version = data[header_field_name]['version']
            generate_out_rec = lambda activity: {header_field_name: data[header_field_name], activity_list_field_name: [activity], 'type': data['type']}

        field_name_tuple = (header_field_name, update_time_field_name, activity_list_field_name)
        
        YYYYMM = data[header_field_name][update_time_field_name][:7].replace('-', '')
        prefix = self.prefix_template.format(**self.feed, year=YYYYMM[:4], month=YYYYMM[-2:])
        
        template = '{identifier}_{beginLocation_roadDirection}_{YYYYMM}_v{version}'
        new_statuses = {
            self.generate_fp(
                template, 
                identifier=self.get_identifier_from_status(feed_version, status),
                beginLocation_roadDirection=self.get_road_direction_from_status(feed_version, status),
                YYYYMM=YYYYMM,
                version=feed_version
            ): status 
            for status in data[activity_list_field_name]
        }
        return new_statuses, generate_out_rec, prefix, field_name_tuple
    
    def get_identifier_from_status(self, feed_version, status):
        if feed_version[0] == '1':
            return status['identifier']
        else:
            return status['properties'].get('road_event_id') or status.get('id')
    
    def get_road_direction_from_status(self, feed_version, status):
        if feed_version[0] == '1':
            return status['beginLocation']['roadDirection']
        elif feed_version[0] == '4':
            return status['properties']['core_details']['direction']
        else:
            return status['properties']['direction']

    def combine_with_existing_recs(self, key, out_rec, field_name_tuple):
        # if not first status for the workzone for the month
        datastream = self.s3helper.get_data_stream(self.bucket, key)
        recs = [json.loads(rec) for rec in datastream.iter_lines()]
        if out_rec == recs[-1]:
            # skip if completely the same as previous record
            self.print_func('Skipped')
            self.n_skipped += 1
            return None
        if len(recs) == 1:
            # if only one record so far, automatically archive first record and save current record
            out_recs = recs + [out_rec]
            self.n_new_status += 1
            self.print_func('Only 1 rec and not the same')
        else:
            # if more than one record, compare current record with previous and previous previous record
            if self.cmp_status(out_rec, recs[-1], recs[-2], field_name_tuple):
                out_recs = recs[:-1] + [out_rec]
                self.n_overwrite += 1
            else:
                out_recs = recs + [out_rec]
                self.n_new_status += 1
        return out_recs

    def cmp_status(self, cur_status, prev_status, prev_prev_status, field_name_tuple):
        """
        Method to check 1) if the current status is retrieved less than one day
        ago compared to the status prior to the previous status, and 2) if the
        current status matches the previous status for all fields except for
        the fields ignored (update_date). Will return false (no overwrite)
        if either condition is false.

        Parameters:
            cur_status: Dictionary object of current work zone activity status
                being compared/ingested.
            prev_status: Dictionary object of previous work zone activity status
                from the same feed and for the same work zone that had been ingested.
            prev_prev_status: Dictionary object of status prior to teh previous status,
                also from the same feed and for the same work zone that had been ingested.
            field_name_tuple: Tuple consisting of field names for feed header (feed
                metadata), last updated timestamp, and activity list, in that order.
        Returns:
            Boolean value showing whether or not the current status should overwrite
            the previous status.
        """
        ignore_keys = ['update_date']
        # consider status as new if last record was at least one day ago
        header_field_name, update_time_field_name, activity_list_field_name = field_name_tuple
        time_diff = dateutil.parser.parse(cur_status[header_field_name][update_time_field_name]) - dateutil.parser.parse(prev_prev_status[header_field_name][update_time_field_name])
        if time_diff >= timedelta(days=1):
            return False

        # if last record is more recent, consider status as new only if any non-ignored field is different
        cur_status = {k:v for k,v in cur_status[activity_list_field_name][0].items() if k not in ignore_keys}
        prev_status = {k:v for k,v in prev_status[activity_list_field_name][0].items() if k not in ignore_keys}
        return cur_status == prev_status