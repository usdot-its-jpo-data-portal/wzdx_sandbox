from datetime import datetime, timedelta
import dateutil.parser
import requests

from s3_helper import S3Helper


class WorkZoneSandbox(object):
    def __init__(self, feed, bucket, aws_profile=None, logger=False):
        self.prefix_template = 'state={state}/feedName={feedName}/year={year}/month={month}/'
        self.feed = feed
        self.bucket = bucket
        self.aws_profile = aws_profile
        self.print_func = print
        if logger:
            self.print_func = logger.info

        self.s3helper = S3Helper(aws_profile=aws_profile)

        self.n_new_status = 0
        self.n_overwrite = 0
        self.n_new_fps = 0
        self.n_skipped = 0

    def generate_fp(self, status, parsed_meta):
        template = '{identifier}_{beginLocation_roadDirection}_{YYYYMM}_v{version}'
        params = {
            'identifier': status['identifier'],
            'beginLocation_roadDirection': status['beginLocation']['roadDirection']
        }
        return template.format(**params, **parsed_meta)

    def cmp_status(self, cur_status, prev_status, prev_prev_status):
        ignore_keys = ['timestampEventUpdate']
        if prev_prev_status:
            # consider status as new if last record was at least one day ago
            time_diff = dateutil.parser.parse(cur_status['timestampEventUpdate']) - dateutil.parser.parse(prev_prev_status['timestampEventUpdate'])
            if time_diff >= timedelta(days=1):
                return False

        # if last record is more recent, consider status as new only if any non-ignored field is different
        cur_status = {k:v for k,v in cur_status.items() if k not in ignore_keys}
        prev_status = {k:v for k,v in prev_status.items() if k not in ignore_keys}
        return cur_status == prev_status

    def update_from_feed(self):
        r = requests.get(self.feed['url'])
        wzdx = r.json()['WZDx']
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
                datastream = self.s3helper.get_data_stream(self.bucket, key)
                recs = [rec for rec in self.s3helper.newline_json_rec_generator(datastream)]
                if out_rec == recs[-1]:
                    # skip if completely the same as previous record
                    print('Skipped')
                    self.n_skipped += 1
                    continue

                prev_status = recs[-1]['WorkZoneActivity'][0]
                prev_prev_status = None
                if len(recs) > 1:
                    prev_prev_status = recs[-2]['WorkZoneActivity'][0]

                if len(recs) == 1:
                    out_recs = recs + [out_rec]
                    self.n_new_status += 1
                    print('Only 1 rec and not the same')
                elif self.cmp_status(current_status, prev_status, prev_prev_status):
                    out_recs = recs[:-1] + [out_rec]
                    self.n_overwrite += 1
                else:
                    out_recs = recs + [out_rec]
                    self.n_new_status += 1
            else:
                out_recs = [out_rec]
                self.n_new_fps += 1

            self.s3helper.write_recs(out_recs, self.bucket, key)

        self.print_func('{} status found in {} feed: {} skipped, {} updates, {} overwrites, {} new files'.format(
        len(new_statuses), self.feed['feedName'], self.n_skipped, self.n_new_status, self.n_overwrite, self.n_new_fps))
