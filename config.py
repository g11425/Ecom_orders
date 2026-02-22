import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
OUTPUT_DIR_INVALID_EVENTS = os.path.join(PROJECT_ROOT, "output_invalid_events")
S3_BUCKET_NAME = "s3-giam-bucket-002"
S3_OUTPUT_DIR = f"s3://{S3_BUCKET_NAME}/Ecom_orders/flink-output/unloaded/"
S3_OUTPUT_DIR_INVALID_EVENTS = f"s3://{S3_BUCKET_NAME}/Ecom_orders/flink-output-invalid-events/unloaded/"
S3_OUTPUT_DIR_FINAL = f"s3://{S3_BUCKET_NAME}/Ecom_orders/flink-output/loaded/"
S3_OUTPUT_DIR_INVALID_EVENTS_FINAL = f"s3://{S3_BUCKET_NAME}/Ecom_orders/flink-output-invalid-events/loaded/"



if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

if not os.path.exists(OUTPUT_DIR_INVALID_EVENTS):
    os.makedirs(OUTPUT_DIR_INVALID_EVENTS)

console_messages = {}

def clean_print(ds):

    os.system('cls' if os.name == 'nt' else 'clear')
    for dicts in console_messages.keys():
        print(f"{dicts}: ")
        for event_type, count in console_messages[dicts].items():
            print(f"{event_type}: {count}")
    sys.stdout.flush()
