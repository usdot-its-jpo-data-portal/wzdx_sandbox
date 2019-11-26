"""
Ingest from WZDx

"""
from __future__ import print_function

import json
import logging
import os
import traceback

from wzdx_sandbox import WorkZoneSandbox


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

FEED = os.environ['FEED']
BUCKET = os.environ['BUCKET']


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """
    # TODO: eventually have this be triggered by another lambda
    wzdx_sandbox = WorkZoneSandbox(feed=json.loads(FEED), bucket=BUCKET, logger=logger)
    wzdx_sandbox.update_from_feed()

    logger.info('Processed events')


if __name__ == '__main__':
    lambda_handler()
