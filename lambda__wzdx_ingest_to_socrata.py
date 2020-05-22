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

logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging


SOCRATA_PARAMS = json.loads(os.environ.get('SOCRATA_PARAMS', ''))


if None in [BUCKET]:
    logger.error('Required ENV variable(s) not found. Please make sure you have specified the following ENV variables: BUCKET')
    exit()


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """
    # load and initialize data flattener based on schema version
    flattener_class = load_flattener(f'wzdx/V{event['feed']['version']}')
    flattener = flattener_class()

    # load and parse data
    wzdx_sandbox = WorkZoneSandbox(feed=event['feed'], bucket=None, logger=logger)
    datastream = wzdx_sandbox.s3helper.get_data_stream(event['bucket'], event['key'])
    data = wzdx_sandbox.parse_to_json(datastream.data.decode('utf-8'))
    current_updated_time = data['road_event_feed_info']['feed_update_date'][:19]

    # check if socrata data is stale
    # section does not work for wzdx v1 feeds
    dataset_id = event['feed']['socratadatasetid']
    dataset = SocrataDataset(dataset_id=dataset_id, socrata_params=SOCRATA_PARAMS)
    sample_current_records = dataset.client.get(dataset_id, limit=1)
    last_updated_time = sample_current_records[0]['feed_update_date'][:19]

    if not current_updated_time > last_updated_time:
        logger.info(f'No update needed - feed has not been updated since {last_updated_time}')
        return

    # feed content is newer than what is in Socrata
    working_id = dataset.create_new_draft()
    flattened_recs = flattener.process_and_split(data)
    response = dataset.clean_and_upsert(flattened_recs, working_id)
    logger.info(response)
    so_ingestor.publish_draft(working_id)
    logger.info(f'New draft for dataset {working_id} published.')
    return


if __name__ == '__main__':
    lambda_handler()
