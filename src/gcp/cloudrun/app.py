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

    if not data or 'name' not in data or 'bucket' not in data:
        return 'Invalid notification', 400

    bucket_name = data['bucket']
    file_name = data['name']
    process_file(bucket_name, file_name)

    return jsonify({'status': 'File processed.'}), 200

def process_file(bucket_name, file_name):
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_string().decode('utf-8')

    rows = list(csv.DictReader(content.splitlines()))
    errors = client.insert_rows_json("oh_practice.run_table", rows)

    if errors:
        print("Errors:", errors)
    else:
        print("New rows have been added.")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
