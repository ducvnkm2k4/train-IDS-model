import pandas as pd


def load_cic_2017_data():
    print("Loading CICIDS 2017 data...")
    cic_2017_data1 = pd.read_parquet('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.parquet')
    cic_2017_data2 = pd.read_parquet('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.parquet')
    cic_2017_data3 = pd.read_parquet('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Morning.pcap_ISCX.parquet')
    cic_2017_data4 = pd.read_parquet('src/dataset/CIC-IDS-2017/Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.parquet')
    cic_2017_data5 = pd.read_parquet('src/dataset/CIC-IDS-2017/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.parquet')
    cic_2017_data6 = pd.read_parquet('src/dataset/CIC-IDS-2017/Wednesday-workingHours.pcap_ISCX.parquet')
    cic_2017_data7 = pd.read_parquet('src/dataset/CIC-IDS-2017/Tuesday-WorkingHours.pcap_ISCX.parquet')
    cic_2017_data8 = pd.read_parquet('src/dataset/CIC-IDS-2017/Monday-WorkingHours.pcap_ISCX.parquet')
    print("CICIDS 2017 data loaded successfully and begin process.")
    cic_2017_data = pd.concat([
        cic_2017_data1, cic_2017_data2, cic_2017_data3, cic_2017_data4, 
        cic_2017_data5, cic_2017_data6, cic_2017_data7, cic_2017_data8
        ], ignore_index=True)
    cic_2017_data = cic_2017_data.drop(columns=[' Fwd Header Length.1'])

    cic_2017_data.to_parquet('src/output/data_raw/cic_2017.parquet', index=False)

    return cic_2017_data

def load_cic_2018_data(label):
    files=[
        'src/dataset/CSE-CIC-IDS2018/Friday-02-03-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Friday-16-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Friday-23-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Thuesday-20-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Thursday-01-03-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Thursday-15-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Thursday-22-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Wednesday-14-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Wednesday-21-02-2018_TrafficForML_CICFlowMeter.parquet',
        'src/dataset/CSE-CIC-IDS2018/Wednesday-28-02-2018_TrafficForML_CICFlowMeter.parquet'
    ]
    cic_2018_data = pd.DataFrame()
    for file_name in files:
        data = pd.read_parquet(file_name)
        data = data.drop(columns=['Protocol','Timestamp'])
        if "Thuesday-20-02-2018" in file_name:
            data = data.drop(columns=['Flow ID', 'Src IP', 'Src Port', 'Dst IP',])
        if label == 'attack':
            cic_2018_data = pd.concat([cic_2018_data, data[data['Label']!='Benign']], ignore_index=True)
        else:
            cic_2018_data = pd.concat([cic_2018_data, data[data['Label']=='Benign']], ignore_index=True)
    print(f'columns: {cic_2018_data.columns} length: {len(cic_2018_data.columns)}')
    print(cic_2018_data.head())
    if label == 'attack':
        cic_2018_data.to_parquet('src/output/data_raw/cic_2018_all_attack.parquet', index=False)
    else:
        cic_2018_data.to_parquet('src/output/data_raw/cic_2018_all_benign.parquet', index=False)
    return cic_2018_data

def data_integration():
    columns = ['destination_port', 'flow_duration',
           'total_fwd_packets', 'total_bwd_packets',
           'total_length_fwd_packets', 'total_length_bwd_packets',
           'fwd_packet_length_max', 'fwd_packet_length_min',
           'fwd_packet_length_mean', 'fwd_packet_length_std',
           'bwd_packet_length_max', 'bwd_packet_length_min',
           'bwd_packet_length_mean', 'bwd_packet_length_std',
           'flow_bytes/s', 'flow_packets/s',
           'flow_iat_mean', 'flow_iat_std', 'flow_iat_max', 'flow_iat_min',
           'fwd_iat_total', 'fwd_iat_mean', 'fwd_iat_std',
           'fwd_iat_max', 'fwd_iat_min',
           'bwd_iat_total', 'bwd_iat_mean', 'bwd_iat_std',
           'bwd_iat_max', 'bwd_iat_min',
           'fwd_psh_flags', 'bwd_psh_flags',
           'fwd_urg_flags', 'bwd_urg_flags',
           'fwd_header_length', 'bwd_header_length',
           'fwd_packets/s', 'bwd_packets/s',
           'packet_length_min', 'packet_length_max',
           'packet_length_mean', 'packet_length_std', 'packet_length_var',
           'fin_flag_count', 'syn_flag_count', 'rst_flag_count',
           'psh_flag_count', 'ack_flag_count', 'urg_flag_count',
           'cwe_flag_count', 'ece_flag_count',
           'down/up_ratio', 'packet_size_avg',
           'fwd_segment_size_avg', 'bwd_segment_size_avg',
           'fwd_bytes/bulk_avg', 'fwd_packet/bulk_avg', 'fwd_bulk_rate_avg',
           'bwd_bytes/bulk_avg', 'bwd_packet/bulk_avg', 'bwd_bulk_rate_avg',
           'subflow_fwd_packets', 'subflow_fwd_bytes',
           'subflow_bwd_packets', 'subflow_bwd_bytes',
           'init_win_bytes_fwd', 'init_win_bytes_bwd',
           'act_data_packet_fwd', 'min_segment_size_fwd',
           'act_mean', 'act_std', 'act_max', 'act_min',
           'idle_mean', 'idle_std', 'idle_max', 'idle_min',
           'label']
    cic_2017_data = load_cic_2017_data()
    cic_2018_data = load_cic_2018_data('attack')
    cic_2017_data.columns = columns
    cic_2018_data.columns = columns
    # Merge the two datasets
    merged_data = pd.concat([cic_2017_data, cic_2018_data], ignore_index=True)
    merged_data = merged_data.drop_duplicates()
    # Save the merged data to a new file
    merged_data.to_parquet('src/output/data_raw/merged_cic_data.parquet', index=False)
    print("Data integration completed.")     
if __name__ == "__main__":
    data_integration()
    load_cic_2018_data('benign')
    print("Data processing completed.")