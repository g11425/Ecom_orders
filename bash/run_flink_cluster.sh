#!/bin/bash
# Start the Flink cluster

source ~/stream/bin/activate 
export PYTHONPATH=/home/ec2-user/Ecom_orders:/home/ec2-user/  
export PATH=/home/ec2-user/stream/lib/python3.10/site-packages/pyflink/bin:$PATH
cd /home/ec2-user/Ecom_orders/
flink run -py flink/RT_process.py --pyexec ~/stream/bin/python --pyFiles config.py
