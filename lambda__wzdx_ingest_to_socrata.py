"""
Ingest and parse WZDx feed data to Work Zone Data Sandbox.

"""
import json
import logging
import os
import traceback

from wzdx_sandbox import WorkZoneSandbox
from sandbox_exporter.flattener import load_flattener
from sandbox_exporter.socrata_util import SocrataDataset
from sandbox_exporter.flattener_wzdx import WzdxV2Flattener, WzdxV3Flattener

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging


SOCRATA_PARAMS = json.loads(os.environ.get('SOCRATA_PARAMS', ''))


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """

    # load and parse data
    wzdx_sandbox = WorkZoneSandbox(feed=event['feed'], bucket=None, logger=logger)
    datastream = wzdx_sandbox.s3helper.get_data_stream(event['bucket'], event['key'])
    data = wzdx_sandbox.parse_to_json(datastream.data.decode('utf-8'))

    # load and initialize data flattener based on schema version
    # flattener_class = load_flattener('wzdx/V{}'.format(event['feed']['version']))
    if int(event['feed']['version']) == 2:
        flattener_class = WzdxV2Flattener
        current_updated_time = data['road_event_feed_info']['feed_update_date'][:19]
    elif int(event['feed']['version']) == 3:
        flattener_class = WzdxV3Flattener
        current_updated_time = data['road_event_feed_info']['update_date'][:19]
    flattener = flattener_class()

    # check if socrata data is stale
    # section does not work for wzdx v1 feeds
    dataset_id = event['feed']['socratadatasetid']
    dataset = SocrataDataset(dataset_id=dataset_id, socrata_params=SOCRATA_PARAMS)
    sample_current_records = dataset.client.get(dataset_id, limit=1)
    if sample_current_records:
        last_updated_time = sample_current_records[0]['feed_update_date'][:19]
        if not current_updated_time > last_updated_time:
            logger.info(f'No update needed - feed has not been updated since {last_updated_time}')
            return

    # feed content is newer than what is in Socrata
    flattened_recs = flattener.process_and_split(data)
    if flattened_recs:
        working_id = dataset.create_new_draft()
        response = dataset.clean_and_upsert(flattened_recs, working_id)
        logger.info(response)
        dataset.publish_draft(working_id)
        logger.info(f'New draft for dataset {working_id} published.')
    else:
        logger.info(f'No records in feed - will not update Socrata dataset')
    return


if __name__ == '__main__':
    lambda_handler()
