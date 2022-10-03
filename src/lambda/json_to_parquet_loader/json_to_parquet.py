#!/usr/bin/env python3

from __future__ import print_function
import sys
import os
import json
import base64
import traceback
import arrow
import awswrangler as wr
import pandas as pd

S3_BUCKET = os.getenv('S3_BUCKET', '')
S3_PATH = os.getenv('S3_PATH', '')
TABLE_NAME = os.getenv('TABLE_NAME', '')
DATABASE_NAME = os.getenv('DATABASE_NAME', '')
CREATE_PARTITION = True if os.getenv('CREATE_PARTITION', '').lower() == 'true' else False

# TODO: fill TYPE_MAP
TYPE_MAP = {
    'service_id': 'int64',
    'timestamp': 'int64'
}


def parse(df, utc_basic_time):
    is_parsing_failed = False

    try:
        df = df.astype(TYPE_MAP)
        df['utc_basic_time'] = utc_basic_time
        df['utc_time'] = pd.to_datetime(df['timestamp'], unit='ms')
    except Exception as e:
        print(f'[ERROR] log parsing failed - {e}', file=sys.stderr)
        is_parsing_failed = True

    return df, is_parsing_failed


def load_to_s3(record_list):
    utc_basic_time = arrow.utcnow().format('YYYY-MM-DD-HH')
    path = f's3://{S3_BUCKET}/{S3_PATH}'

    df = pd.DataFrame(record_list)
    df, parsing_failed = parse(df, utc_basic_time)

    if not parsing_failed:
        wr.s3.to_parquet(
            df=df,
            path=path,
            compression=None,
            dataset=True,
            partition_cols=['utc_basic_time']
        )

        print(f'[INFO] Record Loading to {path} end', file=sys.stderr)
        if CREATE_PARTITION:
            wr.catalog.add_parquet_partitions(
                database=DATABASE_NAME,
                table=TABLE_NAME,
                partitions_values={f'{path}/utc_basic_time={utc_basic_time}/': [utc_basic_time]}
            )
            print(f'[INFO] create partition "{path}/utc_basic_time={utc_basic_time}/" succeed', file=sys.stderr)


def do_task(event, context):
    record_list = []
    print(f'[INFO] Record count: {len(event["Records"])}', file=sys.stderr)
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        json_object = json.loads(payload)
        record_list.append(json_object)

    partition_length = 100
    partitioned_record_list = [record_list[i:i+partition_length] for i in range(0, len(record_list), partition_length)]

    for records in partitioned_record_list:
        load_to_s3(records)


def lambda_handler(event, context):
    try:
        print(f'[INFO] Task started', file=sys.stderr)
        do_task(event, context)
    except Exception as e:
        print(f'[ERROR] Lambda failed : {e}', file=sys.stderr)
        traceback.print_exc()
