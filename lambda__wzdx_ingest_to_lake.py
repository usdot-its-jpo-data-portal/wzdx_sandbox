"""
Ingest and parse WZDx feed data to Work Zone Data Sandbox.

"""
from __future__ import print_function

import json
import logging
import os
import traceback

from wzdx_sandbox import WorkZoneSandbox


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

BUCKET = os.environ['BUCKET']


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """
    wzdx_sandbox = WorkZoneSandbox(feed=event['feed'], bucket=BUCKET, logger=logger)
    datastream = wzdx_sandbox.s3helper.get_data_stream(event['bucket'], event['key'])
    wzdx_sandbox.ingest(data=datastream.data.decode('utf-8'))

    logger.info('Processed events')


if __name__ == '__main__':
    lambda_handler()
