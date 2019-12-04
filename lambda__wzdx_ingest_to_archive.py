"""
Ingest from WZDx

"""
from __future__ import print_function

import json
import logging
import os
import traceback

from wzdx_sandbox import WorkZoneRawSandbox


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

BUCKET = os.environ['BUCKET']
LAMBDA_TO_TRIGGER = os.environ['LAMBDA_TO_TRIGGER']


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """
    wzdx_sandbox = WorkZoneRawSandbox(feed=event['feed'], bucket=BUCKET,
                    lambda_to_trigger=LAMBDA_TO_TRIGGER, logger=logger)
    wzdx_sandbox.ingest()

    logger.info('Processed events')


if __name__ == '__main__':
    lambda_handler()
