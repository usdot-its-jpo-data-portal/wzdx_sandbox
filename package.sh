#!/bin/bash
echo "Remove current package wzdx_ingest_to_archive.zip"
rm -rf wzdx_ingest_to_archive.zip
pip install -r lambda__wzdx_ingest_to_lake__requirements.txt --upgrade --target package/
cp lambda__wzdx_ingest_to_archive.py s3_helper.py wzdx_sandbox.py package/
mv package/lambda__wzdx_ingest_to_archive.py package/lambda_function.py
cd package && zip -r ../wzdx_ingest_to_archive.zip * && cd ..
rm -rf package
echo "Created package in wzdx_ingest_to_archive.zip"

echo "Remove current package wzdx_ingest_to_lake.zip"
rm -rf wzdx_ingest_to_lake.zip
pip install -r lambda__wzdx_ingest_to_lake__requirements.txt --upgrade --target package/
cp lambda__wzdx_ingest_to_lake.py s3_helper.py wzdx_sandbox.py package/
mv package/lambda__wzdx_ingest_to_lake.py package/lambda_function.py
cd package && zip -r ../wzdx_ingest_to_lake.zip * && cd ..
rm -rf package
echo "Created package in wzdx_ingest_to_lake.zip"

echo "Remove current package wzdx_ingest_to_socrata.zip"
rm -rf wzdx_ingest_to_socrata.zip
pip install -r lambda__wzdx_ingest_to_socrata__requirements.txt --upgrade --target package/
cp lambda__wzdx_ingest_to_socrata.py s3_helper.py wzdx_sandbox.py package/
mv package/lambda__wzdx_ingest_to_socrata.py package/lambda_function.py
cd package && zip -r ../wzdx_ingest_to_socrata.zip * && cd ..
rm -rf package
echo "Created package in wzdx_ingest_to_socrata.zip"
