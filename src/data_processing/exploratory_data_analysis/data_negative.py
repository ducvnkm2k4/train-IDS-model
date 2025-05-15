import pandas as pd
import numpy as np

def data_negative(data, dataset_name):
    # Chỉ xét các cột số, bỏ qua 'label'
    numeric_cols = data.select_dtypes(include=[np.number]).columns
    numeric_cols = [col for col in numeric_cols if col != 'label']

    # Đếm số lượng bản ghi có giá trị âm trong từng cột
    negative_counts = (data[numeric_cols] < 0).sum()

    # Lọc ra cột có ít nhất 1 giá trị âm
    negative_counts = negative_counts[negative_counts > 0]

    # Ghi report ra file
    report_path = f"src/output/data_analysis/negative_values_report_{dataset_name}.csv"
    negative_counts.to_csv(report_path, header=["negative_count"])

    print(f"Negative value report for '{dataset_name}' saved to {report_path}.")
    return negative_counts

if __name__ == "__main__":
    data_train = pd.read_parquet('src/output/data_processed/data_train.parquet')
    data_test = pd.read_parquet('src/output/data_processed/data_test.parquet')

    print("Generating data report for train dataset...")
    report_train = data_negative(data_train, 'train')

    print("Generating data report for test dataset...")
    report_test = data_negative(data_test, 'test')

    print("Data reports generated.")
