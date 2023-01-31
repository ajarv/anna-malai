from statistics import mode
from pycaret.anomaly import * 
import numpy as np

# model can be iforest, histogram etc. ..
# check  https://towardsdatascience.com/time-series-anomaly-detection-with-pycaret-706a6e2b2427
def detect_anomalies(data,model='histogram'):
    if model == 'laymans_way':
        return detect_anomalies_laymans_way_01(data)
    print(f'begin detect_anomalies( {data.shape} )')
    _data = data.copy()
    data.set_index('timestamp', drop=True, inplace=True)
    # creature features from date
    data['day'] = [i.day for i in data.index]
    data['day_name'] = [i.day_name() for i in data.index]
    data['day_of_year'] = [i.dayofyear for i in data.index]
    data['week_of_year'] = [i.weekofyear for i in data.index]
    data['is_weekday'] = [i.isoweekday() for i in data.index]
    
    s = setup(data, session_id = 123, verbose= False)

    iforest = create_model(model, fraction = 0.2)
    iforest_results = assign_model(iforest)
    iforest_results['timestamp'] = _data['timestamp']
    return iforest_results


def detect_anomalies_laymans_way(data):
    data['MA7'] = data['MetricValue'].astype(float).rolling(7).mean()+ 0.0001
    data['FactorOff'] = 10*(data['MA7'] - data['MetricValue']).abs()/data['MA7']
    data['Anomaly_Score'] = (np.log2(data['FactorOff']))/3
    data['Anomaly'] = data['Anomaly_Score'] > 0
    return data

def detect_anomalies_laymans_way_01(data):
    data['MA7'] = data['MetricValue'].astype(float).rolling(7).mean()+ 0.0001
    data['MA7D'] = data['MA7'].diff().fillna(0)
    data['Anomaly_Score'] = (np.log2(10* (data['MA7D']/data['MA7']).abs().rolling(7).mean() ))/3
    data['Anomaly'] = data['Anomaly_Score'] > 0
    return data

def detect_anomalies_hist_ma(data,value_column='MetricValue'):
    data_copy = data.copy()
    if data.shape[0] < 30:
        print(f"Can not detect anomalies with fewer than a month's reads")
        data_copy['Anomaly'] = False
        data_copy['Anomaly_Score'] = -1
        return data_copy

    data['day_name'] = data['timestamp'].apply(lambda t: t.day_name())
    data['MA7'] = data[value_column].rolling(7).mean().fillna(0)+0.0001
    data = data[data['day_name'] == 'Saturday']
    data_copy_w = data.copy().reset_index()
    data['MA7WD'] = data['MA7'].diff().fillna(0)
    data = data[['timestamp','MA7WD']]
    data.set_index('timestamp', drop=True, inplace=True)
    if data.shape[0] < 4:
        print(f"Can not detect anomalies with fewer than a four weeks's reads")
        data_copy['Anomaly'] = False
        data_copy['Anomaly_Score'] = -1
        return data_copy
    
    s = setup(data, session_id = 123,verbose=False)
    model = create_model('histogram', fraction = 0.1)
    model_results = assign_model(model)
    
    model_results['timestamp'] = data_copy_w['timestamp'].apply(lambda x: x.strftime('%Y-%V'))
    
    results_df = model_results[['timestamp','Anomaly', 'Anomaly_Score']].set_index('timestamp')
    
    orig_columns = list(data_copy.columns)

    data_copy['timestamp_yw'] = data_copy['timestamp'].apply(lambda x: x.strftime('%Y-%V'))

    data_with_anomaly_info =  data_copy.join(results_df,on='timestamp_yw').fillna(-1)
    data_with_anomaly_info['Anomaly'] = data_with_anomaly_info['Anomaly'] == 1

    data_with_anomaly_info =  data_with_anomaly_info[ orig_columns + ['Anomaly', 'Anomaly_Score']]
    return data_with_anomaly_info