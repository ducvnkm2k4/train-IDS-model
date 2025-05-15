import pandas as pd
from sklearn.model_selection import train_test_split


def split_data(data, test_size=0.2, random_state=42):
    
    data_train,data_test = train_test_split(data, test_size=test_size, random_state=random_state)
    print(f"Train data shape: {data_train.shape}")
    print(f"Test data shape: {data_test.shape}")
    data_train.to_parquet('src/output/data_raw/data_train_test/data_train.parquet', index=False)
    data_test.to_parquet('src/output/data_raw/data_train_test/data_test.parquet', index=False)

if __name__ == "__main__":
    # Load the data
    data = pd.read_parquet('src/output/data_raw/merged_cic_data.parquet')
    
    split_data(data)
    
    print("Data splitting completed.")