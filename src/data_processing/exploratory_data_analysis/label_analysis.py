import pandas as pd
import matplotlib.pyplot as plt

cic_2017_data1 = pd.read_csv('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv')
cic_2017_data2 = pd.read_csv('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv')
cic_2017_data3 = pd.read_csv('src/dataset/CIC-IDS-2017/Friday-WorkingHours-Morning.pcap_ISCX.csv')
cic_2017_data4 = pd.read_csv('src/dataset/CIC-IDS-2017/Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv')
cic_2017_data5 = pd.read_csv('src/dataset/CIC-IDS-2017/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv')
cic_2017_data6 = pd.read_csv('src/dataset/CIC-IDS-2017/Wednesday-workingHours.pcap_ISCX.csv')
cic_2017_data7 = pd.read_csv('src/dataset/CIC-IDS-2017/Tuesday-WorkingHours.pcap_ISCX.csv')
cic_2017_data8 = pd.read_csv('src/dataset/CIC-IDS-2017/Monday-WorkingHours.pcap_ISCX.csv')
# Đọc dữ liệu
cic_2017_data = pd.read_csv('src/output/data_processed/nids_data.csv')
# cic_2018_data = pd.read_csv('src/output/data_raw/cic_2018.csv')

# Kiểm tra xem cột 'destination_port' có chứa giá trị NaN hay không
print(cic_2017_data['destination_port'].isnull().sum())  # Kiểm tra số lượng NaN trong cột

# Xử lý giá trị NaN nếu có
cic_2017_data = cic_2017_data.dropna(subset=['destination_port'])

# Kiểm tra kiểu dữ liệu của cột 'destination_port'
print(cic_2017_data['destination_port'].dtype)

# Đảm bảo cột 'destination_port' là kiểu int
cic_2017_data['destination_port'] = cic_2017_data['destination_port'].astype(int)

# Tính tần suất xuất hiện của các cổng đích (destination ports)
port_counts = cic_2017_data['destination_port'].value_counts()
port_counts.to_csv('src/output/data_raw/port_counts.csv', index=True)
print(port_counts.sort_values(ascending=True))
# # Vẽ biểu đồ thanh (bar chart)
# plt.figure(figsize=(10, 6))
# port_counts.plot(kind='bar', color='skyblue')
# plt.title('Frequency ofdestination_ports in CICIDS 2017')
# plt.xlabel('Destination Port')
# plt.ylabel('Frequency')
# plt.show()
