"""
Ingest raw data from WZDx feed to Work Zone Raw Data Sandbox.

"""
import json
import logging
import os
import traceback

from wzdx_sandbox.wzdx_sandbox import WorkZoneRawSandbox


logger = logging.getLogger()
logger.setLevel(logging.INFO)  # necessary to make sure aws is logging

BUCKET = os.environ.get('BUCKET')
LAMBDA_TO_TRIGGER = os.environ.get('LAMBDA_TO_TRIGGER')
SOCRATA_LAMBDA_TO_TRIGGER = os.environ.get('SOCRATA_LAMBDA_TO_TRIGGER')


if None in [BUCKET, LAMBDA_TO_TRIGGER]:
    logger.error('Required ENV variable(s) not found. Please make sure you have specified the following ENV variables: BUCKET, LAMBDA_TO_TRIGGER')
    exit()


def lambda_handler(event=None, context=None):
    """AWS Lambda handler. """
    wzdx_sandbox = WorkZoneRawSandbox(feed=event['feed'], bucket=BUCKET,
                    lambda_to_trigger=LAMBDA_TO_TRIGGER,
                    socrata_lambda_to_trigger=SOCRATA_LAMBDA_TO_TRIGGER,
                    logger=logger)
    wzdx_sandbox.ingest()


if __name__ == '__main__':
    lambda_handler()
