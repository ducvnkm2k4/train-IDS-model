import pandas as pd

def label_analysis(data):

    # Count the number of unique labels
    label_counts = data['label'].value_counts()
    
    # Print the label counts
    print("Label Counts:")
    print(label_counts)
    
if __name__ == "__main__":
    data_train = pd.read_parquet('src/output/data_raw/data_train_test/data_train.parquet')
    data_test = pd.read_parquet('src/output/data_raw/data_train_test/data_test.parquet')

    print("Train Data Label Analysis:")
    label_analysis(data_train)
    print("\nTest Data Label Analysis:")
    label_analysis(data_test)
    print("Label analysis completed.")


    
