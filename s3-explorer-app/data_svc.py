import pandas as pd


def pipeline_bucket_type(bucket_name):
    return "sequencing" if ('sequ' in bucket_name and 'archive' not in bucket_name) else \
        "archive" if ('archive' in bucket_name) else \
            "results" if ('results' in bucket_name) else \
                "fastq" if ('fastq' in bucket_name) else \
                    "working" if ('working' in bucket_name) else \
                        "cache" if ('cache' in bucket_name) else "non-pipeline"


def get_s3_df():
    BASE_PATH = '/Users/avashisth/workspace/aws-util/cost-explorer/data'
    gaccts = ['clinical', 'grail-sysinfra-eng', 'eng', 'grail-sysinfra-prod', 'grail-prod-galleri', 'msk',
              'aws-grail-sequence-data-archives', 'grail-prod-mrd']
    df = pd.concat([
        pd.read_csv(f'{BASE_PATH}/metrics/{account}/s3-storage-metrics.csv.gz')
        for account in gaccts
    ]).reset_index()
    df['timestamp'] = pd.to_datetime(df['Timestamp'])
    df['PipelineBucketType'] = df['BucketName'].apply(pipeline_bucket_type)
    df = df[['timestamp', 'Bytes', 'BucketName', 'StorageType', 'AWSAccount', 'GrailAccount', 'PipelineBucketType']]
    df['DailyCost'] = df.apply(get_daily_cost,axis=1)
    return df


RATES = {
    "StandardStorage": 0.021 * .5,
    "StandardIAStorage": 0.0125 * 0.6,
    "DeepArchiveStorage": 0.00099 * 0.7,
    "IntelligentTieringFAStorage": 0.021 * .5,
    "IntelligentTieringIAStorage": 0.0125 * 0.6,
    "IntelligentTieringAIAStorage": 0.004 * 0.7,
    "GlacierStorage": 0.00099 * 0.7,
    "IntelligentTieringAAStorage": 0.021,
    "IntelligentTieringDAAStorage": 0.00099 * 0.7
}


def get_daily_cost(row):
    return RATES.get(row['StorageType'], 0.0) * row['Bytes'] * 1e-9 / 30
    # return  (
    #             (sz_da * 0.00099 * 0.7 * 12 * 1e6) +  # Deep Archive  @ 0.00099 /GB/MONTH x (1- 30% GRAIL disc)
    #             (sz_aia * 0.004 * 0.7 * 12 * 1e6) +  # Archive Instant Access  @ 0.04 x (1- 30% GRAIL disc)
    #             (sz_ia * 0.0125 * 0.6 * 12 * 1e6) +  # Infrequent Access  @ 0.0125 /GB/MONTH x (1- 40% GRAIL disc)
    #             (sz_fa * 0.022 * .5 * 12 * 1e6) +  # Frequent Access  @ 0.022 /GB/MONTH x (1- 50% GRAIL disc)
    #             (sz_ss * 0.022 * .5 * 12 * 1e6)  # Frequent Access  @ 0.022 /GB/MONTH x (1- 50% GRAIL disc)
    #     )


if __name__ == '__main__':
    df = get_s3_df()
    print(df.head())
    import json
    # print(json.dumps(dict([(i, 0.021) for i in df.StorageType.unique() if 'Storage' in i]), indent='\t'))
