#!/bin/bash

endpoint_profile="--endpoint-url=http://localhost:4566 --profile=localstack"
download_dir=/Users/flat9th/workspace/目利きデータ/local-test

keyname=journal-filter-data
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=sales-itmcd
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=sales-itmcd-start-ymd
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

exit 0
