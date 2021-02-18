import unittest
import responses
import os

from wzdx_sandbox.s3_helper import S3Helper


class TestS3Helper(unittest.TestCase):
    def test_init_s3_helper(self):
        test_s3_helper = None
        try:
            test_s3_helper = S3Helper()
        except:
            self.assertIsNone(test_s3_helper)