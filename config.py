import os
import posixpath
import sys


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
OUTPUT_DIR_INVALID_EVENTS = os.path.join(PROJECT_ROOT, "output_invalid_events")
# S3_BUCKET_NAME = "s3-giam-bucket-002"
# S3_RAW_OUTPUT_PREFIX = "Ecom_orders/flink-output/raw/unloaded/"
# S3_VALID_EVENTS_OUTPUT_PREFIX = "Ecom_orders/flink-output/valid-events/unloaded/"
# S3_INVALID_EVENTS_OUTPUT_PREFIX = "Ecom_orders/flink-output/invalid-events/unloaded/"
# S3_VALID_EVENTS_OUTPUT_FINAL_PREFIX = "Ecom_orders/flink-output/valid-events/loaded/"
# S3_INVALID_EVENTS_OUTPUT_FINAL_PREFIX = "Ecom_orders/flink-output/invalid-events/loaded/"
# S3_RAW_OUTPUT_DIR = f"s3://{S3_BUCKET_NAME}/{S3_RAW_OUTPUT_PREFIX}"
# S3_OUTPUT_DIR_VALID_EVENTS = f"s3://{S3_BUCKET_NAME}/{S3_VALID_EVENTS_OUTPUT_PREFIX}"
# S3_OUTPUT_DIR_INVALID_EVENTS = f"s3://{S3_BUCKET_NAME}/{S3_INVALID_EVENTS_OUTPUT_PREFIX}"
# S3_OUTPUT_DIR_VALID_EVENTS_FINAL = f"s3://{S3_BUCKET_NAME}/{S3_VALID_EVENTS_OUTPUT_FINAL_PREFIX}"
# S3_OUTPUT_DIR_INVALID_EVENTS_FINAL = f"s3://{S3_BUCKET_NAME}/{S3_INVALID_EVENTS_OUTPUT_FINAL_PREFIX}"


WINDOW_SIZE = 30  # seconds
WATERMARK_DELAY = 5  # seconds


S3_BUCKET_NAME = "s3-giam-bucket-002"

S3_RAW_OUTPUT_PREFIX = posixpath.join(
    "Ecom_orders", "flink-output", "raw", "unloaded"
)

S3_VALID_EVENTS_OUTPUT_PREFIX = posixpath.join(
    "Ecom_orders", "flink-output", "valid-events", "unloaded"
)

S3_INVALID_EVENTS_OUTPUT_PREFIX = posixpath.join(
    "Ecom_orders", "flink-output", "invalid-events", "unloaded"
)

S3_VALID_EVENTS_OUTPUT_FINAL_PREFIX = posixpath.join(
    "Ecom_orders", "flink-output", "valid-events", "loaded"
)

S3_INVALID_EVENTS_OUTPUT_FINAL_PREFIX = posixpath.join(
    "Ecom_orders", "flink-output", "invalid-events", "loaded"
)

S3_RAW_OUTPUT_DIR = f"s3://{S3_BUCKET_NAME}/{S3_RAW_OUTPUT_PREFIX}"
S3_OUTPUT_DIR_VALID_EVENTS = f"s3://{S3_BUCKET_NAME}/{S3_VALID_EVENTS_OUTPUT_PREFIX}"
S3_OUTPUT_DIR_INVALID_EVENTS = f"s3://{S3_BUCKET_NAME}/{S3_INVALID_EVENTS_OUTPUT_PREFIX}"
S3_OUTPUT_DIR_VALID_EVENTS_FINAL = f"s3://{S3_BUCKET_NAME}/{S3_VALID_EVENTS_OUTPUT_FINAL_PREFIX}"
S3_OUTPUT_DIR_INVALID_EVENTS_FINAL = f"s3://{S3_BUCKET_NAME}/{S3_INVALID_EVENTS_OUTPUT_FINAL_PREFIX}"
S3_CHECKPOINT_DIR = f"s3://{S3_BUCKET_NAME}/Ecom_orders/flink-checkpoints/"

REDSHIFT_TBL_CREATE_SQL_PATH = {
    "valid_events": os.path.join(PROJECT_ROOT, "sql/create/create_target_table.sql"),
    "invalid_events": os.path.join(PROJECT_ROOT, "sql/create/create_invalid_target_table.sql"),
    "orders": os.path.join(PROJECT_ROOT, "sql/create/create_order_table.sql"),
    "staging": os.path.join(PROJECT_ROOT, "sql/create/create_staging_table.sql"),
    "tracker": os.path.join(PROJECT_ROOT, "sql/create/create_tracking_table.sql")
}

REDSHIFT_TBL_LOAD_SQL_PATH = {
    "orders": os.path.join(PROJECT_ROOT, "sql/load/update_order_table.sql")
}  

REDSHIFT_TABLES = {
    "valid_events": "public.order_events",
    "invalid_events": "public.invalid_order_events",
    "orders": "public.orders",
    "staging": "public.order_events_staging",
    "tracker": "public.s3_load_tracker"
}

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
