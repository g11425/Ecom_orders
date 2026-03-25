#!/bin/bash
# start flink custer

source ~/stream/bin/activate
export PYTHONPATH=/home/ec2-user/Ecom_orders:/home/ec2-user/  
export PATH=/home/ec2-user/stream/lib/python3.10/site-packages/pyflink/bin:$PATH

/home/ec2-user/stream/lib/python3.10/site-packages/pyflink/bin/start-cluster.sh