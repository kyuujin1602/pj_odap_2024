import csv
import json
import time
import random
from confluent_kafka import Producer

# Cấu hình Kafka Producer
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'csv-producer'
}
producer = Producer(producer_config)

# Định nghĩa topic
topic = 'credit_card_transactions'

# Đọc file CSV
file_path = 'D:/Users/Desktop/HCMUS_HK7/4_ODAP/BaiTapThucHanh/Week910/Data/credit_card_transactions-ibm_v2.csv'
with open(file_path, mode='r') as file:
    # Đọc bằng DictReader để lưu giá trị header
    csv_reader = csv.DictReader(file)

    for row in csv_reader:
        # Chuyển dữ liệu thành JSON
        message = json.dumps(row)
        # Gửi dữ liệu
        producer.produce(topic, value=message)
        print(f'Sent: {message}')
        
        # Thời gian trễ ngẫu nhiên
        time.sleep(random.randint(1, 3))

# Xử lý dữ liệu tồn đọng trước khi kết thúc
producer.flush()
print("All messages sent!")
