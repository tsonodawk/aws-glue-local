#!/bin/bash

endpoint_profile="--endpoint-url=http://localhost:4566 --profile=localstack"
download_dir=/Users/flat9th/workspace/目利きデータ/local-test

keyname=journal-filter-data
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=new-release-items
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=popular-amount-abc
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=popular-items
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=sales-shop-itmcd
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=sales-start-day
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

keyname=score-early-purchase
aws s3 cp s3://${keyname} ${download_dir}/${keyname} --recursive ${endpoint_profile}

exit 0
