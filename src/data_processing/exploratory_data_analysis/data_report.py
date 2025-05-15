import pandas as pd
from sklearn.preprocessing import LabelEncoder
def data_report(data):
    encoder = LabelEncoder()
    data[' Label'] = encoder.fit_transform(data[' Label'])
    # Tạo bảng thống kê cơ bản  
    report = data.describe().T
    report["median"] = data.median()
    report["dtype"] = data.dtypes
    report["nan"] = data.isnull().sum()

    # Làm tròn các giá trị số đến 2 chữ số thập phân
    report = report.round(3)

    # Reset index để đưa tên đặc trưng thành một cột
    report = report.reset_index()
    report = report.rename(columns={"index": "feature_name"})

    # In báo cáo
    print(report)
    return report

if __name__ == "__main__":
    data_train = pd.read_parquet('src/output/data_processed/data_train.parquet')
    data_test = pd.read_parquet('src/output/data_processed/data_test.parquet')
    print("Generating data report for train datasets...")
    report = data_report(data_train)
    report.to_excel('src/output/data_analysis/data_train_report.xlsx', index=False)
    print("Generating data report for test dataset...")
    report = data_report(data_test)
    report.to_excel('src/output/data_analysis/data_report_test.xlsx', index=False)

    
    print("Data report generated.")