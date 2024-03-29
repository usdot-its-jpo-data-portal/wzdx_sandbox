#!/bin/bash
echo "Remove current package wzdx_ingest_to_archive.zip"
rm -rf wzdx_ingest_to_archive.zip
pip install -r lambda__wzdx_ingest_to_lake__requirements.txt --upgrade --target package/
cp lambda__wzdx_ingest_to_archive.py package/
cp WZDx_URLs.csv package/
cp -r wzdx_sandbox/ package/wzdx_sandbox
mv package/lambda__wzdx_ingest_to_archive.py package/lambda_function.py
cd package && zip -r ../wzdx_ingest_to_archive.zip * && cd /home/m29810/wzdx/wzdx_sandbox
rm -rf package
echo "Created package in wzdx_ingest_to_archive.zip"

echo "Remove current package wzdx_ingest_to_lake.zip"
rm -rf wzdx_ingest_to_lake.zip
pip install -r lambda__wzdx_ingest_to_lake__requirements.txt --upgrade --target package/
cp lambda__wzdx_ingest_to_lake.py package/
cp -r wzdx_sandbox/ package/wzdx_sandbox
mv package/lambda__wzdx_ingest_to_lake.py package/lambda_function.py
cd package && zip -r ../wzdx_ingest_to_lake.zip * && cd /home/m29810/wzdx/wzdx_sandbox
rm -rf package
echo "Created package in wzdx_ingest_to_lake.zip"

echo "Remove current package wzdx_ingest_to_socrata.zip"
rm -rf wzdx_ingest_to_socrata.zip
pip install -r lambda__wzdx_ingest_to_socrata__requirements.txt --upgrade --target package/
cp lambda__wzdx_ingest_to_socrata.py package/
cp -r wzdx_sandbox/ package/wzdx_sandbox
mv package/lambda__wzdx_ingest_to_socrata.py package/lambda_function.py
cd package && zip -r ../wzdx_ingest_to_socrata.zip * && cd /home/m29810/wzdx/wzdx_sandbox
rm -rf package
echo "Created package in wzdx_ingest_to_socrata.zip"
