#!/bin/bash
# Start the Flink cluster job

source ~/stream/bin/activate 
export PYTHONPATH=/home/ec2-user/Ecom_orders:/home/ec2-user/  
export PATH=/home/ec2-user/stream/lib/python3.10/site-packages/pyflink/bin:$PATH
cd /home/ec2-user/Ecom_orders/
mkdir -p /home/ec2-user/stream/lib/python3.10/site-packages/pyflink/plugins/s3/
cp jar/flink-s3-fs-hadoop-2.2.0.jar /home/ec2-user/stream/lib/python3.10/site-packages/pyflink/plugins/s3/
flink run -py flink/RT_process.py --pyexec ~/stream/bin/python --pyFiles config.zip
