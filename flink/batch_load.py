import psycopg2
import boto3
from collections import defaultdict
import config as cfg
from botocore.exceptions import ClientError
import json


# ---------------- CONFIG ----------------
REDSHIFT_HOST = ""
REDSHIFT_DB = ""
REDSHIFT_USER = ""
REDSHIFT_PASSWORD = ""
REDSHIFT_PORT = 5439

S3_BUCKET = cfg.S3_BUCKET_NAME
UNLOADED_PREFIX = cfg.S3_VALID_EVENTS_OUTPUT_PREFIX
LOADED_PREFIX = cfg.S3_VALID_EVENTS_OUTPUT_FINAL_PREFIX

INVALID_UNLOADED_PREFIX = cfg.S3_INVALID_EVENTS_OUTPUT_PREFIX
INVALID_LOADED_PREFIX = cfg.S3_INVALID_EVENTS_OUTPUT_FINAL_PREFIX

IAM_ROLE = "arn:aws:iam::478106802666:role/service-role/AmazonRedshift-CommandsAccessRole-20260113T204825"
TARGET_TABLE = cfg.REDSHIFT_TABLES["valid_events"]
STAGING_TABLE = cfg.REDSHIFT_TABLES["staging"]
TRACK_TABLE = cfg.REDSHIFT_TABLES["tracker"]
ORDER_TABLE = cfg.REDSHIFT_TABLES["orders"]
INVALID_EVENT_TABLE = cfg.REDSHIFT_TABLES["invalid_events"]


def load_configs():
    
    global REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER, REDSHIFT_PASSWORD

    secret_name = "Ecom_orders_info"
    region_name = "eu-north-1"


    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']

    secret_dict = json.loads(secret)



    REDSHIFT_HOST = secret_dict['REDSHIFT_HOST']
    REDSHIFT_DB = secret_dict['REDSHIFT_DB']
    REDSHIFT_USER = secret_dict['REDSHIFT_USER']
    REDSHIFT_PASSWORD = secret_dict['REDSHIFT_PASSWORD']

    print("Loaded configs from Secrets Manager")


def setup_redshift_tables(cursor):

    try:
        for sql_file in cfg.REDSHIFT_TBL_CREATE_SQL_PATH.values():
            with open(sql_file, 'r') as f:
                create_sql = f.read()
                cursor.execute(create_sql)
                print(f"Executed: {sql_file}")
    
    except Exception as e:
        print("Error setting up tables:", e)




def batch_load(FINAL_TABLE, PREFIX, FINAL_PREFIX):

    # ---------------- CONNECT S3 ----------------
    s3 = boto3.client("s3", region_name="eu-north-1")
    paginator = s3.get_paginator("list_objects_v2")

    # ---------------- STEP 1: GROUP FILES BY PARTITION ----------------
    partition_files = defaultdict(list)
    partition_has_incomplete = defaultdict(bool)

    is_loaded = ""    

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Skip folder placeholders
            if key.endswith("/"):
                continue

            partition = key.rsplit("/", 1)[0] + "/"
            is_loaded = key.rsplit("/", 6)[1]
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


    setup_redshift_tables(cursor)

    # ---------------- STEP 2: PROCESS EACH PARTITION ----------------
    for partition_prefix, files in partition_files.items():

        partition_key = partition_prefix.replace(PREFIX, "").rstrip("/")

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
            AND is_loaded = '{is_loaded}'
        """, (partition_key,))

        already_loaded = cursor.fetchone()

        if not already_loaded:

            print("Not loaded. Executing COPY.")

            s3_path = f"s3://{S3_BUCKET}/{partition_prefix}"

            copy_sql = f"""
            COPY {STAGING_TABLE}
            FROM '{s3_path}'
            IAM_ROLE '{IAM_ROLE}'
            FORMAT AS CSV
            DELIMITER ','
            IGNOREHEADER 0
            TIMEFORMAT 'auto'
            EMPTYASNULL
            BLANKSASNULL
            TRIMBLANKS
            ACCEPTINVCHARS
            COMPUPDATE OFF
            STATUPDATE OFF;
            """

            target_load_sql = f"""
            INSERT INTO {FINAL_TABLE} 
            SELECT *, '{partition_key}' as partition_key FROM {STAGING_TABLE};
            """

            with open(cfg.REDSHIFT_TBL_LOAD_SQL_PATH["orders"], 'r') as f:
                order_table_sql = f.read()

            clear_staging_sql = f"TRUNCATE TABLE {STAGING_TABLE};"

            loaded_to_target = False

            try:

                cursor.execute(clear_staging_sql)
                print("Cleared staging table.")
                cursor.execute(copy_sql)
                print("COPY executed.")
                cursor.execute(target_load_sql)
                print("Data inserted into target table.")
                cursor.execute(order_table_sql)
                print("Order table updated.")
                loaded_to_target = True

                cursor.execute(f"""
                    INSERT INTO {TRACK_TABLE}
                    (partition_path, status, loaded_at, is_loaded)
                    VALUES (%s, 'SUCCESS', GETDATE(), %s)
                """, (partition_key, is_loaded))



                print("COPY SUCCESS")

            except Exception as e:
                print("COPY FAILED:", e)
                if loaded_to_target:
                    print("Rolling back target table insert...")
                    cursor.execute(f"""
                        DELETE FROM {FINAL_TABLE}
                        WHERE partition_key = %s
                    """, (partition_key,))
                continue

        else:
            print("Already loaded. Moving files only.")

    #    ----- MOVE FILES TO LOADED PREFIX -----
        for key in files:
            new_key = key.replace(PREFIX, FINAL_PREFIX, 1)

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

if __name__ == "__main__":

    load_configs()

    batch_load(TARGET_TABLE, UNLOADED_PREFIX, LOADED_PREFIX)
    batch_load(INVALID_EVENT_TABLE, INVALID_UNLOADED_PREFIX, INVALID_LOADED_PREFIX)
