from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import pyhdfs
import os


# Hàm để lấy danh sách các file trên HDFS
def list_hdfs_files():
    # Kết nối tới HDFS 
    fs = pyhdfs.HdfsClient(hosts='172.18.96.1:9870')

    # Đường dẫn đến thư mục trên HDFS
    directory_path = '/transaction_processing'

    # Lấy danh sách các file trong thư mục
    file_names = fs.listdir(directory_path)

    # In ra tên các file
    for file_name in file_names:
        print(file_name)

    return file_names

# Hàm để đọc dữ liệu từ danh sách các file trên HDFS và nối vào một file CSV
def append_all_files_to_csv(file_names, local_csv_file):
    file_names = [file_name for file_name in file_names if file_name not in ['_SUCCESS', '_temporary']]
    # Kết nối tới HDFS
    fs = pyhdfs.HdfsClient(hosts='172.18.96.1:9870')  # Kết nối HDFS

    for file_name in file_names:
        file_path = f'/transaction_processing/{file_name}'  # Đường dẫn đầy đủ đến file trên HDFS

        # Đọc nội dung file từ HDFS vào pandas DataFrame
        with fs.open(file_path) as f:
            df = pd.read_csv(f)

            # Kiểm tra dữ liệu trước khi lưu (optional)
            print(f"Preview of the data in {file_name}:")
            print(df.head())

            # Nếu file CSV đã tồn tại, mở chế độ 'append' và bỏ qua header
            if os.path.exists(local_csv_file):
                df.to_csv(local_csv_file, mode='a', header=False, index=False)
                print(f"Appended data from {file_name} to {local_csv_file}")
            else:
                # Nếu file chưa tồn tại, tạo file mới
                df.to_csv(local_csv_file, mode='w', index=False)
                print(f"Created {local_csv_file} and added data from {file_name}")
        # Sử dụng subprocess.call()
        # remove_file_command = f"hdfs dfs -rm -r /transaction_processing/{file_name}"
        # # Chạy lệnh và capture output
        # result = subprocess.run(remove_file_command, shell=True, text=True, capture_output=True)
        #
        # client = InsecureClient('172.31.208.1:9870', user='hdfs')

        # Xóa tệp
        fs.delete(file_path, recursive=True)
        print(f"Successfully removed {file_name}")
        # remove_file_command = f"hdfs dfs -rm -r /transaction_processing/{file_name}"
        #
        # # Chạy câu lệnh và lấy kết quả
        # os.system(remove_file_command)




# Hàm để đọc và hiển thị nội dung của một file từ HDFS
# def read_and_show_file_content(file_name):
#     # Kết nối tới HDFS
#     fs = pyhdfs.HdfsClient(hosts='172.31.208.1:9870')  # Kết nối HDFS
#     file_path = f'/transaction/{file_name}'  # Đường dẫn đầy đủ đến file trên HDFS
#
#     # Đọc nội dung file
#     with fs.open(file_path) as f:
#         # Nếu file là CSV, hiển thị nội dung bằng pandas
#         try:
#             df = pd.read_csv(f)
#             print(f"Contents of the file '{file_name}':")
#             print(df.head())  # Hiển thị 5 dòng đầu tiên
#         except Exception as e:
#             print(f"Error reading the file '{file_name}': {e}")

with DAG(
    dag_id="hdfs_to_local",
    schedule="0 0 * * *",  # Chạy mỗi 10 phút
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:


    # Tạo PythonOperator để lấy và in danh sách file
    get_file_list_task = PythonOperator(
        task_id='list_hdfs_files',
        python_callable=list_hdfs_files,
    )

    # Task để nối dữ liệu từ tất cả các file HDFS vào file CSV
    append_all_to_csv_task = PythonOperator(
        task_id='append_all_files_to_csv',
        python_callable=lambda: append_all_files_to_csv(
            list_hdfs_files(),  # Gọi hàm để lấy danh sách file từ HDFS
            'transaction_data.csv'  # File CSV trên local để nối dữ liệu
        ),
    )

    # Đặt thứ tự thực hiện các task
    get_file_list_task >> append_all_to_csv_task