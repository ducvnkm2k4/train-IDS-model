import pandas as pd

def load_nids_data():
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
           'bwd_bytes/bulk_avg', 'bwd_packet/bulk_avg', 'bwd_bytes/bulk_avg',
           'subflow_fwd_packets', 'subflow_fwd_bytes',
           'subflow_bwd_packets', 'subflow_bwd_bytes',
           'init_win_bytes_fwd', 'init_win_bytes_bwd',
           'act_data_packet_fwd', 'min_segment_size_fwd',
           'act_mean', 'act_std', 'act_max', 'act_min',
           'idle_mean', 'idle_std', 'idle_max', 'idle_min',
           'label']
    # Load CICIDS 2017 data
    print("Loading CICIDS 2017 data...")
    cic_2017_data1 = pd.read_csv('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv')
    cic_2017_data2 = pd.read_csv('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv')
    cic_2017_data3 = pd.read_csv('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Morning.pcap_ISCX.csv')
    cic_2017_data4 = pd.read_csv('src/dataset/CIC-IDS-2017/Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv')
    cic_2017_data5 = pd.read_csv('src/dataset/CIC-IDS-2017/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv')
    cic_2017_data6 = pd.read_csv('src/dataset/CIC-IDS-2017/Wednesday-workingHours.pcap_ISCX.csv')
    cic_2017_data7 = pd.read_csv('src/dataset/CIC-IDS-2017/Tuesday-WorkingHours.pcap_ISCX.csv')
    cic_2017_data8 = pd.read_csv('src/dataset/CIC-IDS-2017/Monday-WorkingHours.pcap_ISCX.csv')
    print("CICIDS 2017 data loaded successfully and begin process.")
    cic_2017_data = pd.concat([
        cic_2017_data1, cic_2017_data2, cic_2017_data3, cic_2017_data4, 
        cic_2017_data5, cic_2017_data6, cic_2017_data7, cic_2017_data8
        ], ignore_index=True)
    cic_2017_data = cic_2017_data.drop(columns=[' Fwd Header Length.1'])
    cic_2017_data = cic_2017_data.dropna()
    cic_2017_data.columns = columns
    cic_2017_data.to_csv('src/output/data_raw/cic_2017.csv', index=False)
    cic_2017_data = pd.read_csv('src/output/data_raw/cic_2017.csv').dropna()
    #load cic 2018 data
    print("Loading CICIDS 2018 data...")
    cic_2018_data1 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/Bot.csv').dropna()
    cic_2018_data2 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/Brute Force -Web.csv').dropna()
    cic_2018_data3 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/Brute Force -XSS.csv').dropna()
    cic_2018_data4 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DDOS attack-HOIC.csv').dropna()
    cic_2018_data5 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DDOS attack-LOIC-UDP.csv').dropna()
    cic_2018_data6 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DoS attacks-GoldenEye.csv').dropna()
    cic_2018_data7 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DoS attacks-Hulk.csv').dropna()
    cic_2018_data8 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DoS attacks-SlowHTTPTest.csv').dropna()
    cic_2018_data9 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DoS attacks-Slowloris.csv').dropna()
    cic_2018_data10 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/FTP-BruteForce.csv').dropna()
    cic_2018_data11 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/Infilteration.csv').dropna()
    cic_2018_data12 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/SQL Injection.csv').dropna()
    cic_2018_data13 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/SSH-Bruteforce.csv').dropna()
    cic_2018_data14 = pd.read_csv('src/dataset/CSE-CIC-IDS2018/DDoS attacks-LOIC-HTTP.csv').dropna()
    print("CICIDS 2018 data loaded successfully and begin process.")
    cic_2018=pd.concat([
        cic_2018_data1, cic_2018_data2, cic_2018_data3, cic_2018_data4, 
        cic_2018_data5, cic_2018_data6, cic_2018_data7, cic_2018_data8, 
        cic_2018_data9, cic_2018_data10, cic_2018_data11, cic_2018_data12, 
        cic_2018_data13, cic_2018_data14], ignore_index=True)
    cic_2018 = cic_2018.drop(columns=['Protocol'])
    cic_2018.columns = columns

    cic_2018.to_csv('src/output/data_raw/cic_2018.csv', index=False)

    #merge cic 2017 and cic 2018
    print("Merging CICIDS 2017 and CICIDS 2018 data...")
    data = pd.concat([cic_2017_data, cic_2018], ignore_index=True)
    data.to_csv('src/output/data_processed/nids_data.csv', index=False)
    return data

if __name__ == "__main__":
    load_nids_data()