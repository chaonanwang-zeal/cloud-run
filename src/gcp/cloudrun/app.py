import os
import csv
from flask import Flask, request
from google.cloud import bigquery, storage

app = Flask(__name__)
client = bigquery.Client()
storage_client = storage.Client()

@app.route('/', methods=['POST'])
def index():
    data = request.get_json()

    print(f"Received data: {data}、これはデータです。")  # 记录接收到的数据

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
