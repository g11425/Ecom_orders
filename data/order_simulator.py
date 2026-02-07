import faker
import random
from enum import Enum
from collections import deque
from kafka import KafkaProducer
import time
import json
from datetime import datetime



class EventSimulator:
    def __init__(self):
        self.faker = faker.Faker()
        self.event_list = deque()
        for i in range(50):
            event = self.init_event()
            self.event_list.append(event)
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, default=self.enum_serializer).encode('utf-8'))

    
    def init_event(self):
        order_id = self.faker.uuid4()
        event_type = event_type_enum.ORDER_PENDING
        timestamp = self.faker.date_time()
        payload = self.generate_payload()
        return event(order_id, event_type, timestamp, payload)
    
    def enum_serializer(self, obj): 
        if isinstance(obj, Enum):
            return obj.name
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable" + str(type(obj)))
    
    def generate_payload(self):
        return {
            "customer_id": self.faker.uuid4(),
            "product_id": self.faker.uuid4(),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(10.0, 100.0), 2)
        }
    
    def next_event(self, event_per_sec=10):

        time.sleep(1.0 / event_per_sec)
        
        if not self.event_list:
            return None
        current_event = self.event_list.popleft()
        next_event = current_event.generate_next_event(self.faker.date_time())
        if next_event is None:
            next_event = self.init_event()
        self.event_list.append(next_event)
        self.kafka_producer.send('order_events', value=next_event.__dict__)

    


    

class event_type_enum(Enum):
    ORDER_PENDING = 1
    ORDER_PAID = 2
    ORDER_SHIPPED = 3
    ORDER_DELIVERED = 4
    ORDER_CANCELLED = 5
    ORDER_PAID_CANCELLED = 6
    ORDER_RETURNED = 7
    ORDER_REFUNDED = 8




class event:

    def __init__(self, order_id, event_type, timestamp, payload):
        self.order_id = order_id
        self.event_type = event_type
        self.timestamp = timestamp
        self.payload = payload

    def generate_next_event(self, new_timestamp):
        next_event_type = None
        cancelled_or_returned = random.random() < 0.1

        if self.event_type == event_type_enum.ORDER_PENDING and cancelled_or_returned:
            next_event_type = event_type_enum.ORDER_CANCELLED
        elif self.event_type == event_type_enum.ORDER_PENDING:
            next_event_type = event_type_enum.ORDER_PAID
        elif (self.event_type == event_type_enum.ORDER_PAID or self.event_type == event_type_enum.ORDER_SHIPPED) and cancelled_or_returned:
            next_event_type = event_type_enum.ORDER_PAID_CANCELLED
        elif self.event_type == event_type_enum.ORDER_PAID:
            next_event_type = event_type_enum.ORDER_SHIPPED
        elif self.event_type == event_type_enum.ORDER_SHIPPED:
            next_event_type = event_type_enum.ORDER_DELIVERED
        elif self.event_type == event_type_enum.ORDER_DELIVERED and cancelled_or_returned:
            next_event_type = event_type_enum.ORDER_RETURNED
        elif self.event_type == event_type_enum.ORDER_DELIVERED:
            next_event_type = None
        elif self.event_type == event_type_enum.ORDER_CANCELLED:
            next_event_type = None
        elif self.event_type == event_type_enum.ORDER_RETURNED or self.event_type == event_type_enum.ORDER_PAID_CANCELLED:
            next_event_type = event_type_enum.ORDER_REFUNDED
        elif self.event_type == event_type_enum.ORDER_REFUNDED:
            next_event_type = None

        if next_event_type is None:
            return None
        return event(self.order_id, next_event_type, new_timestamp, self.payload)

