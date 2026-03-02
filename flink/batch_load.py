import psycopg2
import boto3
from collections import defaultdict
import config as cfg

# ---------------- CONFIG ----------------
REDSHIFT_HOST = "aws-user.478106802666.eu-north-1.redshift-serverless.amazonaws.com"
REDSHIFT_DB = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "CHNEDbrmxl657)("
REDSHIFT_PORT = 5439

S3_BUCKET = cfg.S3_BUCKET_NAME
UNLOADED_PREFIX = cfg.S3_VALID_EVENTS_OUTPUT_PREFIX
LOADED_PREFIX = cfg.S3_VALID_EVENTS_OUTPUT_FINAL_PREFIX

IAM_ROLE = "arn:aws:iam::478106802666:role/service-role/AmazonRedshift-CommandsAccessRole-20260113T204825"
TARGET_TABLE = "public.order_events"
TRACK_TABLE = "public.s3_load_tracker"


# ---------------- CONNECT S3 ----------------
s3 = boto3.client("s3", region_name="eu-north-1")
paginator = s3.get_paginator("list_objects_v2")

# ---------------- STEP 1: GROUP FILES BY PARTITION ----------------
partition_files = defaultdict(list)
partition_has_incomplete = defaultdict(bool)


for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=UNLOADED_PREFIX):
    for obj in page.get("Contents", []):
        key = obj["Key"]

        # Skip folder placeholders
        if key.endswith("/"):
            continue

        partition = key.rsplit("/", 1)[0] + "/"

        print(f" keys - {key}")

        # Check for completed CSV
        if key.endswith(".csv") and obj["Size"] > 0:
            partition_files[partition].append(key)
        else:
            # Any non-csv or zero-sized file = incomplete partition
            partition_has_incomplete[partition] = True

for partition in partition_files:
    print(f"partition: {partition} ")
    for files in partition_files[partition]:
        print(f"files: {files} ")


# ---------------- CONNECT REDSHIFT ----------------
conn = psycopg2.connect(
    host=REDSHIFT_HOST,
    port=REDSHIFT_PORT,
    database=REDSHIFT_DB,
    user=REDSHIFT_USER,
    password=REDSHIFT_PASSWORD
)
conn.autocommit = True
cursor = conn.cursor()


# ---------------- STEP 2: PROCESS EACH PARTITION ----------------
for partition_prefix, files in partition_files.items():

    partition_key = partition_prefix.replace(UNLOADED_PREFIX, "").rstrip("/")

    print(f"\nProcessing partition: {partition_key}")

    # Skip if incomplete files detected
    if partition_has_incomplete[partition_prefix]:
        print("⚠ Incomplete files detected. Skipping partition.")
        continue

    # Skip empty partitions
    if not files:
        print("No completed CSV files found. Skipping.")
        continue

    # ----- Check Redshift metadata -----
    cursor.execute(f"""
        SELECT 1 FROM {TRACK_TABLE}
        WHERE partition_path = %s
        AND status = 'SUCCESS'
    """, (partition_key,))

    already_loaded = cursor.fetchone()

    if not already_loaded:

        print("Not loaded. Executing COPY.")

        s3_path = f"s3://{S3_BUCKET}/{partition_prefix}"

        copy_sql = f"""
        COPY {TARGET_TABLE}
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS CSV
        IGNOREHEADER 0
        TIMEFORMAT 'auto'
        EMPTYASNULL
        BLANKSASNULL
        TRIMBLANKS
        ACCEPTINVCHARS
        COMPUPDATE OFF
        STATUPDATE OFF;
        """

        try:
            cursor.execute(copy_sql)

            cursor.execute(f"""
                INSERT INTO {TRACK_TABLE}
                (partition_path, status, loaded_at)
                VALUES (%s, 'SUCCESS', GETDATE())
            """, (partition_key,))

            print("COPY SUCCESS")

        except Exception as e:
            print("COPY FAILED:", e)
            continue

    else:
        print("Already loaded. Moving files only.")

#    ----- MOVE FILES TO LOADED PREFIX -----
    for key in files:
        new_key = key.replace(UNLOADED_PREFIX, LOADED_PREFIX, 1)

        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource={'Bucket': S3_BUCKET, 'Key': key},
            Key=new_key
        )

        s3.delete_object(Bucket=S3_BUCKET, Key=key)

    print("Moved partition to loaded/")

# ---------------- CLEANUP ----------------
cursor.close()
conn.close()