import pandas as pd


def pipeline_bucket_type(bucket_name):
    return "sequencing" if ('sequ' in bucket_name and 'archive' not in bucket_name) else \
        "archive" if ('archive' in bucket_name) else \
        "results" if ('results' in bucket_name) else \
        "fastq" if ('fastq' in bucket_name) else\
        "working"  if ('working' in bucket_name) else\
        "cache" if ('cache' in bucket_name) else "non-pipeline"
def get_s3_df():
    BASE_PATH='/Users/avashisth/workspace/aws-util/cost-explorer/data'
    gaccts = ['clinical', 'grail-sysinfra-eng', 'eng', 'grail-sysinfra-prod', 'grail-prod-galleri', 'msk',
            'aws-grail-sequence-data-archives', 'grail-prod-mrd']
    df = pd.concat([
        pd.read_csv(f'{BASE_PATH}/metrics/{account}/s3-storage-metrics.csv.gz')
        for account in gaccts
    ]).reset_index()
    df['timestamp'] = pd.to_datetime(df['Timestamp'])
    df['PipelineBucketType'] = df['BucketName'].apply(pipeline_bucket_type)
    df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType','AWSAccount', 'GrailAccount','PipelineBucketType']]
    return df