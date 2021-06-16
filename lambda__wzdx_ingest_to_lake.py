"""
Ingest and parse WZDx feed data to Work Zone Data Sandbox.

"""
import json
import logging
import os
import traceback

from wzdx_sandbox.wzdx_sandbox import WorkZoneSandbox


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

BUCKET = os.environ.get('BUCKET')

if None in [BUCKET]:
    logger.error('Required ENV variable(s) not found. Please make sure you have specified the following ENV variables: BUCKET')
    exit()


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """
    try:
        wzdx_sandbox = WorkZoneSandbox(feed=event['feed'], bucket=BUCKET, logger=logger)
        datastream = wzdx_sandbox.s3helper.get_data_stream(event['bucket'], event['key'])
        wzdx_sandbox.ingest(data=datastream.data.decode('utf-8'))
    except:
        print(traceback.format_exc())
        print(event)
        raise

if __name__ == '__main__':
    lambda_handler()
