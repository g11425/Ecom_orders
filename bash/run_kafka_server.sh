#!/bin/bash

source ~/stream/bin/activate 
export PYTHONPATH=/home/ec2-user/Ecom_orders:/home/ec2-user/  

KAFKA_CLUSTER_ID="$(~/kafka/bin/kafka-storage.sh random-uuid)"

~/kafka/bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c ~/kafka/config/server.properties

~/kafka/bin/kafka-server-start.sh ~/kafka/config/server.properties
