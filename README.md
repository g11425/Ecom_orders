#Ecom_orders

#problem statement - 
design a streaming system to process E commerce order events which may have out of order events, duplicate events and also events with inconsistent/ invalid order lifecycle etc. The system should also be fault tolerant if the flink task node crashes, back pressure from DDB over trottles, target sink becomes unavailable. it should also be scalable and be able to handle cold bursts.   

#Problem statement refined - 

Design a fault-tolerant, scalable streaming system to process e-commerce order lifecycle events where:

    * Events may arrive out-of-order
    * Events may be duplicated
    * Events may violate valid order lifecycle transitions
    * Order events for the same order may arrive days apart

The system must handle:

    * Flink task/node crashes
    * DynamoDB throttling / backpressure
    * Sink outages (e.g., Redshift unavailable)
    * Cold bursts (sudden traffic spikes)

The system must provide:

    * Exactly-once semantics
    * Lifecycle validation
    * Invalid event side-output
    * Order status aggregates
    * Durable raw event storage