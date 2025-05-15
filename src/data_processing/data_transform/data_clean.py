import pandas as pd
import numpy as np

def data_clean(data):
    col_del = [
        'fwd_bytes/bulk_avg','fwd_packet/bulk_avg','fwd_bulk_rate_avg','bwd_bytes/bulk_avg',
        'bwd_packet/bulk_avg','bwd_bulk_rate_avg','bwd_psh_flags','bwd_urg_flags'
    ]
    data = data.drop(columns=col_del)
    # Loại bỏ các giá trị vô cực (inf, -inf)
    data.replace([np.inf, -np.inf], np.nan, inplace=True)

    # Loại bỏ các bản ghi chứa NaN sau khi thay thế
    data.dropna(inplace=True)

    # Loại bỏ các bản ghi có destination_port = 0
    data = data[data['destination_port'] != 0]

    # làm sạch giá trị âm cho từng cột
    col_negative = [
        'flow_duration', 'flow_bytes/s', 'flow_packets/s',
        'flow_iat_mean', 'flow_iat_max', 'fwd_iat_min',
        'fwd_header_length', 'bwd_header_length', 'min_segment_size_fwd'
    ]
    
    for col in col_negative:
        data[col] = data[col].mask(data[col] < 0, data[col].mean())  # Thay thế giá trị âm bằng giá trị trung bình của cột

    # làm sạch giá trị âm cho 'flow_iat_min'
    data['flow_iat_min'] = data['flow_iat_min'].mask(data['flow_iat_min'] < 0, data['flow_iat_min'].median())

    # làm sạch giá trị dương vô cùng
    return data


if __name__ == "__main__":
    data_train = pd.read_parquet('src/output/data_processed/data_train.parquet')
    data_test = pd.read_parquet('src/output/data_processed/data_test.parquet')
    data_train = data_clean(data_train)
    data_test = data_clean(data_test)
    data_train.to_parquet('src/output/data_processed/data_train.parquet', index=False)
    data_test.to_parquet('src/output/data_processed/data_test.parquet', index=False)
    print("Data cleaning completed.")
