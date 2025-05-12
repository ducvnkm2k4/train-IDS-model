import pandas as pd

def load_hids_data():
    # Use read_parquet instead of read_csv for Parquet files
    data1 = pd.read_parquet('src/dataset/ember_dataset/test_ember_2018_v2_features.parquet')
    data1.to_csv('src/output/data_raw/HIDS_test.csv', index=False)
    data2 = pd.read_parquet('src/dataset/ember_dataset/train_ember_2018_v2_features.parquet')
    data2.to_csv('src/output/data_raw/HIDS_train.csv', index=False)

if __name__ == "__main__":
    load_hids_data()
