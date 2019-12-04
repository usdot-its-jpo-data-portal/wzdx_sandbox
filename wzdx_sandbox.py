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
    def __init__(self, bucket, aws_profile=None, logger=None):
        self.bucket = bucket
        self.s3helper = S3Helper(aws_profile=aws_profile)
        self.print_func = print
        if logger:
            self.print_func = logger.info

    def generate_fp(self, template, params):
        return template.format(**params)


class WorkZoneRawSandbox(ITSSandbox):
    def __init__(self, bucket, feed=None, lambda_to_trigger=None,
                # registry_dataset_id=None, socrata_params=None,
                **kwargs):
        super(WorkZoneRawSandbox, self).__init__(bucket, **kwargs)
        self.prefix_template = 'state={state}/feedName={feedname}/year={year}/month={month}/'
        self.feed = feed
        self.lambda_to_trigger = lambda_to_trigger
        # self.registry_dataset_id = registry_dataset_id
        # self.socrata_params = socrata_params

    def generate_fp(self, datetimeRetrieved):
        template = '{feedname}_{datetimeRetrieved}'
        params = {
            'feedname': self.feed['feedname'],
            'datetimeRetrieved': datetimeRetrieved
        }
        fp = super(WorkZoneRawSandbox, self).generate_fp(template, params)
        return fp

    def ingest(self):
        datetimeRetrieved = datetime.now()
        r = requests.get(self.feed['url']['url'])

        # write raw feed to raw bucket
        prefix = self.prefix_template.format(**self.feed, year=datetimeRetrieved.strftime('%Y'), month=datetimeRetrieved.strftime('%m'))
        fp = self.generate_fp(datetimeRetrieved)
        self.s3helper.write_bytes(r.content, self.bucket, key=prefix+fp)

        # # update last ingest time to Socrata WZDx feed registry
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
            self.print_func('Skip {}'.format(self.feed['feedname']))
            

class WorkZoneSandbox(ITSSandbox):
    def __init__(self, bucket, feed=None, **kwargs):
        super(WorkZoneSandbox, self).__init__(bucket, **kwargs)
        self.prefix_template = 'state={state}/feedName={feedname}/year={year}/month={month}/'
        self.feed = feed

        self.n_new_status = 0
        self.n_overwrite = 0
        self.n_new_fps = 0
        self.n_skipped = 0

    def generate_fp(self, status, parsed_meta):
        template = '{identifier}_{beginLocation_roadDirection}_{YYYYMM}_v{version}'
        params = deepcopy(parsed_meta)
        params['identifier'] = status['identifier']
        params['beginLocation_roadDirection'] = status['beginLocation']['roadDirection']
        fp = super(WorkZoneSandbox, self).generate_fp(template, params)
        return fp

    def cmp_status(self, cur_status, prev_status, prev_prev_status):
        ignore_keys = ['timestampEventUpdate']
        # consider status as new if last record was at least one day ago
        time_diff = dateutil.parser.parse(cur_status['Header']['timeStampUpdate']) - dateutil.parser.parse(prev_prev_status['Header']['timeStampUpdate'])
        if time_diff >= timedelta(days=1):
            return False

        # if last record is more recent, consider status as new only if any non-ignored field is different
        cur_status = {k:v for k,v in cur_status['WorkZoneActivity'][0].items() if k not in ignore_keys}
        prev_status = {k:v for k,v in prev_status['WorkZoneActivity'][0].items() if k not in ignore_keys}
        return cur_status == prev_status

    def parse_to_json(self, data):
        format = self.feed['format']
        if type(data) == dict:
            out = data
        elif format == 'xml':
            xmldict = xmltodict.parse(data)
            out = json.loads(json.dumps(xmldict))
        elif format == 'json':
            out = json.loads(data)
        else:
            out = data
        return out

    def ingest(self, data):
        data = self.parse_to_json(data)
        # semi-parse feed
        wzdx = data['WZDx']
        meta = {
            'YYYYMM': wzdx['Header']['timeStampUpdate'][:7].replace('-', ''),
            'version': wzdx['Header']['versionNo']
        }

        prefix = self.prefix_template.format(**self.feed, year=meta['YYYYMM'][:4], month=meta['YYYYMM'][-2:])
        new_statuses = {self.generate_fp(status, meta): status for status in wzdx['WorkZoneActivity']}

        for fp, current_status in new_statuses.items():
            key = prefix+fp
            out_rec = {'Header': wzdx['Header'], 'WorkZoneActivity': [current_status]}
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
                    if self.cmp_status(out_rec, recs[-1], recs[-2]):
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
