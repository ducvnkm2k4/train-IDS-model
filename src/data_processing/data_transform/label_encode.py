import pandas as pd
from src.data_processing.exploratory_data_analysis.label_analysis import label_analysis
import pandas as pd
from src.data_processing.exploratory_data_analysis.label_analysis import label_analysis

def label_encode(data):
    label_mapping = {
        'BENIGN': 'BENIGN',
        'DDoS': 'DDoS',
        'DDOS attack-HOIC': 'DDoS',
        'DDoS attacks-LOIC-HTTP': 'DDoS',
        'DDOS attack-LOIC-UDP': 'DDoS',

        'DoS Hulk': 'DoS',
        'DoS attacks-Hulk': 'DoS',
        'DoS GoldenEye': 'DoS',
        'DoS attacks-GoldenEye': 'DoS',
        'DoS attacks-Slowloris': 'DoS',
        'DoS slowloris': 'DoS',
        'DoS Slowhttptest': 'DoS',
        'DoS attacks-SlowHTTPTest': 'DoS',

        'SSH-Bruteforce': 'Brute Force',
        'FTP-Patator': 'Brute Force',
        'SSH-Patator': 'Brute Force',
        'FTP-BruteForce': 'Brute Force',
        'Brute Force -Web': 'Brute Force',

        'PortScan': 'Port Scan',
        'Bot': 'Botnet',

        'Infilteration': 'Infiltration',
        'Infiltration': 'Infiltration',

        'Web Attack � Brute Force': 'Web Attack',
        'Web Attack � XSS': 'Web Attack',
        'Web Attack � Sql Injection': 'Web Attack',
        'Brute Force -XSS': 'Web Attack',
        'SQL Injection': 'Web Attack',

        'Heartbleed': 'Heartbleed'
    }

    # Gán label chuẩn
    data = data.copy()
    data.loc[:, 'label'] = data['label'].map(label_mapping)

    # Loại bỏ nhãn không dùng
    data = data[~data['label'].isin(['Heartbleed', 'Web Attack'])].copy()

    # Hiển thị phân bố nhãn trước encode số
    label_analysis(data)

    label_encode_mapping = {
        'BENIGN': 0,
        'DDoS': 1,
        'DoS': 2,
        'Brute Force': 3,
        'Port Scan': 4,
        'Botnet': 5,
        'Infiltration': 6,
    }

    data.loc[:, 'label'] = data['label'].map(label_encode_mapping)
    return data


if __name__ == "__main__":
    # Load the data
    data_train = pd.read_parquet('src/output/data_raw/data_train_test/data_train.parquet')
    data_test = pd.read_parquet('src/output/data_raw/data_train_test/data_test.parquet')
    
    # Apply label encoding
    data = label_encode(data_train)
    data.to_parquet('src/output/data_processed/data_train.parquet', index=False)
    data_test = label_encode(data_test)
    data_test.to_parquet('src/output/data_processed/data_test.parquet', index=False)

    print(data)
    print(data_test)
    print("Label encoding completed.")
