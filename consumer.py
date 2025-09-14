from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
import pyspark.sql.functions as F
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

# Hàm để lấy tỷ giá USD/VND từ API Vietcombank
def get_exchange_rate():
    url = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            # Phân tích nội dung XML
            root = ET.fromstring(response.content)
            for item in root.findall('Exrate'):
                currency_code = item.get('CurrencyCode')
                if currency_code == 'USD':  # Lấy tỷ giá USD/VND
                    sell_rate = item.get('Sell')
                    sell_rate_cleaned = sell_rate.replace(',', '')  # Loại bỏ dấu phẩy
                    return float(sell_rate_cleaned)  # Chuyển đổi thành số thực
    except Exception as e:
        print(f"Lỗi khi lấy tỷ giá: {e}")
    return 24000  # Trả về tỷ giá mặc định nếu không lấy được từ API

# Lấy tỷ giá cho ngày hôm nay
today_exchange_rate = get_exchange_rate()
print(f"Tỷ giá USD/VND hôm nay: {today_exchange_rate}")

# Cấu hình Spark Session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Định nghĩa schema mới với cột timestamp
schema = StructType([
    StructField("User", StringType(), True),
    StructField("Card", StringType(), True),
    StructField("Year", StringType(), True), 
    StructField("Month", StringType(), True), 
    StructField("Day", StringType(), True),  
    StructField("Time", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("Use Chip", StringType(), True),
    StructField("Merchant Name", StringType(), True),
    StructField("Merchant City", StringType(), True),
    StructField("Merchant State", StringType(), True),
    StructField("Zip", StringType(), True),
    StructField("MCC", StringType(), True),
    StructField("Errors?", StringType(), True),
    StructField("Is Fraud?", StringType(), True)
])

# Đọc dữ liệu từ Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "credit_card_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Giải mã dữ liệu từ Kafka (key và value đều là binary)
decoded_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Chuyển đổi dữ liệu JSON thành DataFrame với schema xác định
json_df = decoded_df.select(F.from_json(col("value"), schema).alias("data")).select("data.*")

# Kiểm tra điều kiện "Is Fraud?" 
success_df = json_df.filter(col("Is Fraud?") == "No")
# Lọc các dòng bị lỗi
error_df = json_df.filter(col("Is Fraud?") == "Yes")

# Hàm xử lý batch lỗi
def process_error_batch(batch_df, batch_id):
    row_count = batch_df.count()  # Đếm số dòng lỗi
    print(f"Số dòng lỗi trong batch {batch_id}: {row_count}")  # In số lượng dòng
    
    if row_count > 0:  # Nếu có dòng lỗi
        batch_df.show(10, truncate=False)  # In chi tiết các dòng lỗi

# Luồng xử lý lỗi 
error_query = error_df.writeStream \
    .foreachBatch(process_error_batch) \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()


# Chuẩn hóa các giá trị Month và Day
success_df = success_df.withColumn(
    "Year", F.lpad(col("Year"), 4, "0")
).withColumn(
    "Month", F.lpad(col("Month"), 2, "0")
).withColumn(
    "Day", F.lpad(col("Day"), 2, "0")
)

# Chuẩn hóa cột Time bằng cách thêm ":00"
success_df = success_df.withColumn(
    "Time",
    F.concat_ws(":", col("Time"), F.lit("00"))  # Thêm ":00" vào cuối giá trị Time
)

# Tạo cột timestamp từ Year, Month, Day và Time
success_df = success_df.withColumn(
    "timestamp",
    to_timestamp(
        F.concat_ws(
            " ",
            F.concat_ws("-", col("Year"), col("Month"), col("Day")),  # Nối Year-Month-Day
            col("Time")  # Thêm Time vào
        ),
        "yyyy-MM-dd HH:mm:ss"  # Định dạng thời gian cuối cùng
    )
)

# Chuyển đổi Amount từ chuỗi thành số
success_df = success_df.withColumn(
    "Amount",
    F.expr("substring(Amount, 2)").cast("double")  # Loại bỏ ký tự đầu tiên (VD: "$") và chuyển thành double
)

# Tính toán cột Amount VND
success_df = success_df.withColumn(
    "Amount VND",
    col("Amount") * today_exchange_rate  
)

# Chọn các cột cần thiết để xuất ra
processed_df = success_df.select("User","Card","Year","Month","Day","Time","Amount VND","timestamp","Merchant Name","Merchant City")

# Lưu các dòng "thành công" xuống HDFS
def save_to_hdfs(batch_df, batch_id):
    # In dữ liệu ra màn hình
    batch_df.show(20, truncate=False)

    # Đếm số dòng đã lưu vào HDFS
    row_count = batch_df.count()
    print(f"Đã lưu {row_count} dòng dữ liệu vào HDFS.")

    batch_df.coalesce(1).write \
        .mode("append") \
        .option("header", True) \
        .csv("hdfs://172.18.96.1:9000/transaction/")

    batch_df.coalesce(1).write \
        .mode("append") \
        .option("header", True) \
        .csv("hdfs://172.18.96.1:9000/transaction_processing/")

query = processed_df.writeStream \
    .foreachBatch(save_to_hdfs) \
    .outputMode("append") \
    .trigger(processingTime="1 minutes") \
    .option("checkpointLocation", "hdfs://172.18.96.1:9000/spark-checkpoint/") \
    .start()

query.awaitTermination()
error_query.awaitTermination()

# 1 phút = 1365 batch 
# 5 phút = 273 batch
# 1 giờ = 34 batch 
# 85 phút = 16 batch 
