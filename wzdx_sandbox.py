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

from s3_helper import S3Helper

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

    def generate_fp(self, template, params):
        return template.format(**params)


class WorkZoneRawSandbox(ITSSandbox):
    """
    Class for working with ITS Work Zone Raw Sandbox.

    """
    def __init__(self, bucket, feed=None, lambda_to_trigger=None,
                # registry_dataset_id=None, socrata_params=None,
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

        # variables necessary to update last ingest time to Socrata WZDx feed registry
        # this is currently done in the previous lambda function "wzdx_trigger_ingest".
        # leaving the block below in case we move the step to this function

        # self.registry_dataset_id = registry_dataset_id
        # self.socrata_params = socrata_params

    def generate_fp(self, datetimeRetrieved):
        """
        Method to generate file name for the raw archive file based on the template,
        feed name, and time the feed was retrieved.

        Parameters:
            datetimeRetrieved: datetime object

        Returns:
            String representing the file name
        """
        template = '{feedname}_{datetimeRetrieved}'
        params = {
            'feedname': self.feed['feedname'],
            'datetimeRetrieved': datetimeRetrieved
        }
        fp = super(WorkZoneRawSandbox, self).generate_fp(template, params)
        return fp

    def ingest(self):
        """
        Method to ingest the raw feed to the ITS Work Zone Raw Sandbox and trigger
        the lambda that will ingest and process the feed further to the ITS Work
        Zone Semi-processed Sandbox.

        """
        datetimeRetrieved = datetime.now()
        r = requests.get(self.feed['url']['url'])

        # write raw feed to raw bucket
        prefix = self.prefix_template.format(**self.feed, year=datetimeRetrieved.strftime('%Y'), month=datetimeRetrieved.strftime('%m'))
        fp = self.generate_fp(datetimeRetrieved)
        self.s3helper.write_bytes(r.content, self.bucket, key=prefix+fp)

        self.print_func('Raw data ingested from {} to {} at {} UTC'.format(self.feed['url']['url'], prefix+fp, datetimeRetrieved))

        # update last ingest time to Socrata WZDx feed registry
        # this is currently done in the previous lambda function "wzdx_trigger_ingest".
        # leaving the block below in case we move the step to this function

        # self.feed['lastingestedtosandbox'] = datetime.now().isoformat()
        # socrata_api = 'https://{domain}/resource/{dataset_id}.json'.format(dataset_id=self.registry_dataset_id, **socrata_params)
        # r = requests.post(socrata_api,
        #                   auth=(socrata_params['username'], socrata_params['password']),
        #                   params={'$$app_token':socrata_params['app_token']},
        #                   data=json.dumps([self.feed]))

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

    def generate_fp(self, status, parsed_meta):
        """
        Method to generate file name for the file based on the work zone status,
        and feed metadata.

        Parameters:
            status: Dictionary object containing one work zone activity status.
                Must have the following fields: identifier, beginLocation.roadDirection.
            parsed_meta: Dictionary object containing pre-parsed feed metadata.
                Must have the following fields: YYYYMM, version

        Returns:
            String representing the file name
        """
        template = '{identifier}_{beginLocation_roadDirection}_{YYYYMM}_v{version}'
        params = deepcopy(parsed_meta)
        if params['version'] == '1':
            params['identifier'] = status['identifier']
            params['beginLocation_roadDirection'] = status['beginLocation']['roadDirection']
        else:
            params['identifier'] = status['properties']['road_event_id']
            params['beginLocation_roadDirection'] = status['properties']['direction']
        fp = super(WorkZoneSandbox, self).generate_fp(template, params)
        return fp

    def cmp_status(self, cur_status, prev_status, prev_prev_status, field_name_tuple):
        """
        Method to check 1) if the current status is retrieved less than one day
        ago compared to the status prior to the previous status, and 2) if the
        current status matches the previous status for all fields except for
        the fields ignored (timestampEventUpdate). Will return false (no overwrite)
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
        ignore_keys = ['timestampEventUpdate']
        # consider status as new if last record was at least one day ago
        headerFieldName, updateTimeFieldName, activityListFieldName = field_name_tuple
        time_diff = dateutil.parser.parse(cur_status[headerFieldName][updateTimeFieldName]) - dateutil.parser.parse(prev_prev_status[headerFieldName][updateTimeFieldName])
        if time_diff >= timedelta(days=1):
            return False

        # if last record is more recent, consider status as new only if any non-ignored field is different
        cur_status = {k:v for k,v in cur_status[activityListFieldName][0].items() if k not in ignore_keys}
        prev_status = {k:v for k,v in prev_status[activityListFieldName][0].items() if k not in ignore_keys}
        return cur_status == prev_status

    def parse_to_json(self, data):
        """
        Method to parse the string data received to json.

        Parameters:
            data: Raw string data from feed archive sandbox. Could be stringified
                JSON object or XML.
        Returns:
            Dictionary object equivalent of the data.
        """
        format = self.feed['format']
        if type(data) == dict:
            out = data
        elif format == 'xml':
            xmldict = xmltodict.parse(data)
            out = json.loads(json.dumps(xmldict))
        elif format in ['json', 'geojson']:
            out = json.loads(data)
        else:
            out = data
        return out

    def ingest(self, data):
        """
        Method to ingest and parse the raw feed from the ITS Work Zone Raw Sandbox
        to the ITS Work Zone Semi-processed Sandbox.

        Parameters:
            data: Raw string data from feed archive sandbox. Could be stringified
                JSON object or XML.
        """
        data = self.parse_to_json(data)
        # semi-parse feed
        if self.feed['version'] == '1':
            # spec version 1
            data = data['WZDx']
            activityListFieldName = 'WorkZoneActivity'
            headerFieldName = 'Header'
            updateTimeFieldName = 'timeStampUpdate'
            feedVersion = data[headerFieldName]['versionNo']
            generate_out_rec = lambda activity: {headerFieldName: data[headerFieldName], activityListFieldName: [activity]}
        else:
            # spec version 2
            activityListFieldName = 'features'
            headerFieldName = 'road_event_feed_info'
            updateTimeFieldName = 'feed_update_date'
            feedVersion = data[headerFieldName]['version']
            generate_out_rec = lambda activity: {headerFieldName: data[headerFieldName], activityListFieldName: [activity], 'type': data['type']}
        field_name_tuple = (headerFieldName, updateTimeFieldName, activityListFieldName)
        meta = {
            'YYYYMM': data[headerFieldName][updateTimeFieldName][:7].replace('-', ''),
            'version': feedVersion
        }
        prefix = self.prefix_template.format(**self.feed, year=meta['YYYYMM'][:4], month=meta['YYYYMM'][-2:])
        new_statuses = {self.generate_fp(status, meta): status for status in data[activityListFieldName]}

        for fp, current_status in new_statuses.items():
            key = prefix+fp
            out_rec = generate_out_rec(current_status)
            if self.s3helper.path_exists(self.bucket, key):
                # if not first status for the workzone for the month
                datastream = self.s3helper.get_data_stream(self.bucket, key)
                recs = [rec for rec in self.s3helper.newline_json_rec_generator(datastream)]
                if out_rec == recs[-1]:
                    # skip if completely the same as previous record
                    print('Skipped')
                    self.n_skipped += 1
                    continue

                if len(recs) == 1:
                    # if only one record so far, automatically archive first record and save current record
                    out_recs = recs + [out_rec]
                    self.n_new_status += 1
                    print('Only 1 rec and not the same')
                else:
                    # if more than one record, compare current record with previous and previous previous record
                    if self.cmp_status(out_rec, recs[-1], recs[-2], field_name_tuple):
                        out_recs = recs[:-1] + [out_rec]
                        self.n_overwrite += 1
                    else:
                        out_recs = recs + [out_rec]
                        self.n_new_status += 1
            else:
                out_recs = [out_rec]
                self.n_new_fps += 1

            self.s3helper.write_recs(out_recs, self.bucket, key)

        self.print_func('{} status found in {} feed: {} skipped, {} overwrites, {} updates, {} new files'.format(
        len(new_statuses), self.feed['feedname'], self.n_skipped, self.n_overwrite, self.n_new_status, self.n_new_fps))
