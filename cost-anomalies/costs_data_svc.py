import sqlalchemy
import pandas as pd
import os
import time 
import datetime
import anomaly_svc
from typing import List



class ComputeUsageAggregator:
    def __init__(self,usage_type_token='SpotUsage'):
        self.usage_type_token = usage_type_token
        self.service = "Amazon Elastic Compute Cloud - Compute"
        
    def get_ut_group_dfs(self,df: pd.DataFrame)->List[pd.DataFrame]:
        service = self.service
        usage_type_token = self.usage_type_token
        df_query = "Service == @service and UsageType.str.contains(@usage_type_token)"
        data = df.query(df_query).copy().reset_index()
        all_usage_types = data['UsageType'].unique().tolist()
        usage_type_groups = set()
        for ut in all_usage_types:
            usage_type_groups.add(ut.split(':')[0])
            # usage_type_groups.add(ut.split('.')[0])
        
        columns = df.columns
        value_cols = ['UnitsUsed','Cost']
        sub_cols = list(set(columns)-set(['UsageType']))
        group_by_cols = list(set(sub_cols) -set(value_cols))

        group_dataframes = []
        for ut_group in usage_type_groups:
            sub_query = "UsageType.str.startswith(@ut_group)"
            sub_data = data.query(sub_query)[sub_cols].groupby(by=group_by_cols).sum().reset_index()
            sub_data['UsageType'] = f"{ut_group}:all.all"
            sub_data = sub_data[columns]
            group_dataframes.append(sub_data)
            
        return group_dataframes
        

    def extend_and_drop(self,df: pd.DataFrame):
        service = self.service
        usage_type_token = self.usage_type_token
        df_query = "not( Service == @service and UsageType.str.contains(@usage_type_token) )"
        group_dataframes = self.get_ut_group_dfs(df)
        return pd.concat([df.query(df_query)]+group_dataframes)


def get_mysql_engine():
    database_username = 'trimble'
    database_password = 'trimble'
    db_url = sqlalchemy.engine.url.URL.create(
        drivername="mysql+mysqlconnector",
        username=database_username,
        password=database_password,
        host='127.0.0.1',
        port=3316,
        database='trimble',
        query={"ssl_ca": '/Users/avashisth/.ssh/rds-combined-ca-bundle.pem'},
    )
    engine = sqlalchemy.create_engine(db_url, pool_recycle=1, pool_timeout=57600)
    return engine

def get_df():
    query = """
        SELECT 
            CONCAT(CallerType,'/',CallerName) Caller ,
            Bucket, 
            GrailAccount,
            SUM(pbam.AccessCount) CallCount  
        FROM PIPELINE_BUCKET_ACCESS_METRICS pbam 
        GROUP BY CallerType,CallerName,AccessType,Bucket,GrailAccount
        ORDER BY CallCount DESC
    """
    df = pd.read_sql(query,get_mysql_engine())
    return df

def get_cnu_df():
    BASE_PATH='/Users/avashisth/workspace/aws-util/cost-explorer/data'
    gaccts = ['clinical', 'grail-sysinfra-eng', 'eng', 'grail-sysinfra-prod', 'grail-prod-galleri', 'msk',
            'aws-grail-sequence-data-archives', 'grail-prod-mrd']
    df = pd.concat([
        pd.read_csv(f'{BASE_PATH}/metrics/{account}/cost-n-usage-x-svc-usetype.csv.gz')
        for account in gaccts
    ])    
    
    dfu = df.query('MetricName == "UsageQuantity"')[['Start', 'GrailAccount', 'SERVICE', 'USAGE_TYPE', 'Unit', 'Amount']]
    dfu.columns = ['Start', 'GrailAccount', 'Service', 'UsageType', 'UsageUnit', 'UnitsUsed']

    dfc = df.query('MetricName == "BlendedCost"')[['Start', 'GrailAccount', 'SERVICE', 'USAGE_TYPE', 'Unit', 'Amount']]
    dfc.columns = ['Start', 'GrailAccount', 'Service', 'UsageType', 'Currency', 'Cost']

    # dfc.shape,dfu.shape


    dfc = dfc.set_index(['Start', 'GrailAccount', 'Service', 'UsageType'])\
        .join(dfu.set_index(['Start', 'GrailAccount', 'Service', 'UsageType']),how='left').reset_index()
    
    dfc['UsageMonth'] = dfc['Start'].apply(lambda x: f'{x[:7]}-01')
    dfc['UsageUnit'] = dfc['UsageUnit'].fillna('NotAvailable')
    dfc['timestamp'] = pd.to_datetime(dfc['Start'])           


    return dfc

# def get_cnu_df():

#     BASE_PATH='/Users/avashisth/workspace/aws-util/cost-explorer/data'
#     gaccts = ['clinical', 'grail-sysinfra-eng', 'eng', 'grail-sysinfra-prod', 'grail-prod-galleri', 'msk',
#             'aws-grail-sequence-data-archives', 'grail-prod-mrd']
#     df = pd.concat([
#         pd.read_csv(f'{BASE_PATH}/metrics/{account}/cost-n-usage-x-svc-usetype.csv.gz')
#         for account in gaccts
#     ])    
#     dfc = df.query('MetricName == "BlendedCost"').reset_index()
#     dfu = df.query('MetricName == "UsageQuantity"').reset_index()

#     dfc['UsageUnit'] = dfu['Unit']
#     dfc['UnitsUsed'] = dfu['Amount']
#     dfc['UsageMonth'] = dfc['Start'].apply(lambda x: f'{x[:7]}-01')
#     dfc['timestamp'] = pd.to_datetime(dfc['Start'])           

#     dfc = dfc[~dfc['UsageUnit'].isnull()].reset_index()

#     dfc = dfc[['timestamp','Start','UsageMonth', 'End', 'Account', 'GrailAccount', 'SERVICE',
#            'USAGE_TYPE', 'Unit', 'Amount', 'UsageUnit', 'UnitsUsed']]
#     dfc.columns = ['timestamp','Start', 'UsageMonth','End', 'Account', 'GrailAccount', 'Service',
#            'UsageType', 'Unit', 'Cost', 'UsageUnit', 'UnitsUsed']
#     return dfc

# def get_cnu_df():
#     BASE_PATH='/Users/avashisth/workspace/aws-util/cost-explorer/data'
#     gaccts = ['clinical', 'grail-sysinfra-eng', 'eng', 'grail-sysinfra-prod', 'grail-prod-galleri', 'msk',
#             'aws-grail-sequence-data-archives', 'grail-prod-mrd']
#     df = pd.concat([
#         pd.read_csv(f'{BASE_PATH}/metrics/{account}/cost-n-usage-x-svc-usetype.csv.gz')
#         for account in gaccts
#     ])    
#     dfu = df.query('MetricName == "BlendedCost"').copy()
#     dfu.columns = ['Start', 'End', 'Account', 'GrailAccount', 'Service', 'UsageType',
#         'MetricName', 'Unit', 'Cost']
#     dfu['timestamp'] = pd.to_datetime(dfu['Start'])
#     return dfu

def get_cnu_df_with_grouped_usages():
    spotUsageAggregator = ComputeUsageAggregator('SpotUsage')
    boxUsageAggregator = ComputeUsageAggregator('BoxUsage')
    df = get_cnu_df()
    df = spotUsageAggregator.extend_and_drop(df)
    df = boxUsageAggregator.extend_and_drop(df)
    return df

def get_anomalies(df,grail_acct,service,usage_type,model='laymans_way'):
    df_query = "GrailAccount == @grail_acct"+\
        " and Service == @service  "+\
        " and UsageType == @usage_type " 
    data = df.query(df_query)[['timestamp','GrailAccount','Service','UsageType','Cost']].reset_index()
    data_with_anomaly_info = anomaly_svc.detect_anomalies(data[['timestamp','Cost']],model=model)
    data['Anomaly'] = data_with_anomaly_info['Anomaly'] == 1 
    data['Anomaly_Score'] = data_with_anomaly_info['Anomaly_Score']
    return data

def get_anomalies_using_ma(df,grail_acct,service,usage_type):
    df_query = "GrailAccount == @grail_acct"+\
        " and Service == @service  "+\
        " and UsageType == @usage_type" 
    data = df.query(df_query).reset_index()[df.columns]
    data_with_anomaly_info = anomaly_svc.detect_anomalies_hist_ma(data,'Cost')
    return data_with_anomaly_info


cnu_with_anomaly_info_path = '/Users/avashisth/sandbox/R/cnu_with_anomaly_info.csv.gz'
def get_cnu_df_with_anomaly_info():
    if os.path.exists(cnu_with_anomaly_info_path) \
        and time.time() - os.path.getmtime(cnu_with_anomaly_info_path) < 3600*24 :
        cnu_with_anomaly_info = pd.read_csv(cnu_with_anomaly_info_path)
        cnu_with_anomaly_info['timestamp'] = pd.to_datetime(cnu_with_anomaly_info['timestamp'])
        return cnu_with_anomaly_info

    df = get_cnu_df_with_grouped_usages()
    significant_svc_ut_df = df[['Service', 'UsageType', 'GrailAccount', 'Cost']]\
            .groupby(by=['Service', 'UsageType', 'GrailAccount'])\
            .sum().reset_index()\
            .query('Cost > 0')\
            .sort_values(by='Cost',ascending=False).copy()

    # sv = ('AWS CloudTrail')
    # ac = ('eng',)
    # ut = ('USW2-PaidEventsRecorded')
    # significant_svc_ut_df = significant_svc_ut_df.query('Service in @sv and GrailAccount in @ac and UsageType in @ut')

    df_with_anomalies_info_list = []
    donex = set()
    for index, row in significant_svc_ut_df.iterrows():
        grail_account = row['GrailAccount']
        service = row['Service']
        usage_type = row['UsageType']
        k = f'{grail_account} | {service} | {usage_type}'
        if k in donex:
            print("xxx Repeating xxx",k)
        donex.add(k)
        print(f'{grail_account} | {service} | {usage_type}')
        
        df_with_anomalies_info_list.append(get_anomalies_using_ma(df,grail_account,service,usage_type))
    cnu_with_anomaly_info = pd.concat(df_with_anomalies_info_list)\
        .sort_values(by=['timestamp','Service','UsageType'])
    cnu_with_anomaly_info.to_csv(cnu_with_anomaly_info_path,index=False)
    return cnu_with_anomaly_info


def get_significant_svc_df(startD,endD):
    df = get_cnu_df_with_anomaly_info()
    significant_svc_ut_df = df.query('timestamp >= @startD and timestamp < @endD')\
        [['Service', 'UsageType', 'GrailAccount', 'UsageUnit','Cost','Anomaly']]\
            .groupby(by=['Service', 'UsageType', 'GrailAccount', 'UsageUnit'])\
            .sum().reset_index()\
            .query('Cost > 0')\
            .sort_values(by='Cost',ascending=False).copy()

    return significant_svc_ut_df


if __name__ == '__main__':
    # df = get_cnu_df_with_grouped_usages()
    # print(df.head())

    df = get_cnu_df_with_anomaly_info()
    print(df.head())
    # date_after = dfu['timestamp'].max()-datetime.timedelta(days=61)
    # print(dfu.query('timestamp > @date_after').head().iloc[0]['timestamp'].strftime('%Y-%m-%d'))
    # print(dfu.tail())
    # dfu = get_cnu_df()
    # endD = dfu['timestamp'].max()
    # startD = endD-datetime.timedelta(days=61)
    # significant_svc_df = get_significant_svc_df(startD,endD)
    # print(significant_svc_df.head())