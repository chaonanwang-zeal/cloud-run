import os
import csv
from flask import Flask, request, jsonify
from google.cloud import bigquery, storage

app = Flask(__name__)
client = bigquery.Client()
storage_client = storage.Client()

@app.route('/', methods=['POST'])
def index():
    data = request.get_json()

    print(f"Received data: {data}")  # 记录接收到的数据

    if not data or 'name' not in data or 'bucket' not in data:
        print("Invalid notification")
        return 'Invalid notification', 400

    bucket_name = data['bucket']
    file_name = data['name']
    process_file(bucket_name, file_name)

    return jsonify({'status': 'File processed.'}), 200

def process_file(bucket_name, file_name):
    print(f"Processing file: {file_name} in bucket: {bucket_name}")  # 记录处理的文件信息

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_string().decode('utf-8')

    rows = list(csv.DictReader(content.splitlines()))
    print(f"Rows to insert: {rows}")  # 记录要插入的行

    errors = client.insert_rows_json("oh_practice.run_table", rows)

    if errors:
        print("Errors:", errors)
    else:
        print("New rows have been added.")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
