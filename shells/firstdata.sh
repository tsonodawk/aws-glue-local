#!/bin/bash

endpoint_profile="--endpoint-url=http://localhost:4566 --profile=localstack"

# バケット作成
echo "========================"
echo "make bucket"
echo "========================"
aws s3 mb s3://test-bucket ${endpoint_profile}
aws s3 mb s3://aws-glue-local-test-bucket ${endpoint_profile}
aws s3 mb s3://dcom-bucket ${endpoint_profile}

# mekiki
aws s3 mb s3://mekiki-data-bucket ${endpoint_profile}
aws s3 mb s3://journal-filter-data ${endpoint_profile}
aws s3 mb s3://visit_count_shop_ymd ${endpoint_profile}
aws s3 mb s3://new-release-items ${endpoint_profile}
aws s3 mb s3://popular-amount-abc ${endpoint_profile}
aws s3 mb s3://popular-quantity-abc ${endpoint_profile}
aws s3 mb s3://popular-items ${endpoint_profile}
aws s3 mb s3://sales-shop-itmcd ${endpoint_profile}
aws s3 mb s3://sales-start-day ${endpoint_profile}
aws s3 mb s3://score-early-purchase ${endpoint_profile}


# データコピー
echo "========================"
echo "copy data"
echo "========================"
datadir=~/OneDrive/work/DCom_データコム/202010xx_目利き/07_サンプルデータ・素材等/sns-pos
aws s3 cp ${datadir}/store_master.txt s3://test-bucket/sns-store_master/ ${endpoint_profile}
aws s3 cp ${datadir}/visit_count_data_by_hour_20190506_20190526.csv s3://test-bucket/sns-visit_count_data_by_hour/ ${endpoint_profile}
aws s3 cp ${datadir}/sales_receipt_data_20190506_20190526.csv s3://test-bucket/sns-receipt_data/ ${endpoint_profile}

# 実データ候補
datadir=/Users/flat9th/workspace/目利きデータ/data
aws s3 cp ${datadir}/journal_0536_2006.csv s3://dcom-bucket/yamanaka_journal_data/ ${endpoint_profile}

# 実データ
aws s3 cp ${datadir}/journal_0676_2006_filter.csv s3://mekiki-data-bucket/mekiki-data/input-output/journal-data/ ${endpoint_profile}
aws s3 cp ${datadir}/journal_0683_2006_filter.csv s3://mekiki-data-bucket/mekiki-data/input-output/journal-data/ ${endpoint_profile}
aws s3 cp ${datadir}/item_filter.csv s3://mekiki-data-bucket/mekiki-data/input-output/item-master/ ${endpoint_profile}
aws s3 cp ${datadir}/category.csv s3://mekiki-data-bucket/mekiki-data/input-output/category-master/ ${endpoint_profile}


datadir=/Users/flat9th/GitRepos/github-tsonodawk/local-dockers/pyspark_jupyter/data
aws s3 cp ${datadir}/parameter-master/parameter.csv s3://mekiki-data-bucket/mekiki-data/input-output/parameter-master/ ${endpoint_profile}
# score_masterは未使用
# aws s3 cp ${datadir}/score_master_early_purchase/score_master_early_purchase.csv s3://mekiki-data-bucket/mekiki-data/input-output/score-master-early-purchase/ ${endpoint_profile}
aws s3 cp ${datadir}/journal-filter-data/*.csv s3://mekiki-data-bucket/mekiki-data/input-output/jounal-filter-data/ ${endpoint_profile}


# dynamodbテーブル作成・データ投入
echo "========================"
echo "make dynamodb table"
echo "========================"
aws dynamodb create-table \
    --table-name aws-glue-local-test-table \
    --attribute-definitions \
        AttributeName=Id,AttributeType=S \
    --key-schema AttributeName=Id,KeyType=HASH \
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 \
    ${endpoint_profile}

echo "========================"
echo "put dynamodb data"
echo "========================"
aws dynamodb put-item \
    --table-name aws-glue-local-test-table  \
    --item \
        '{"Id": {"S": "test"}, "Column1": {"S": "test1"}, "Column2": {"S": "test2"}, "Column3": {"S": "test3"}}' \
    ${endpoint_profile}

# テーブル一覧
echo "========================"
echo "check tables"
echo "========================"
aws dynamodb list-tables ${endpoint_profile}

# データ一覧
echo "========================"
echo "check data"
echo "========================"
aws dynamodb scan --table-name aws-glue-local-test-table ${endpoint_profile}

# バケット確認
echo "========================"
echo "check bucket"
echo "========================"
aws s3 ls ${endpoint_profile}

