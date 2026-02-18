import faker
import random
from enum import Enum
from collections import deque
from kafka import KafkaProducer
import time
import json
from datetime import datetime
from events import event, event_type_enum



class EventSimulator:

    def __init__(self, config=None):
        if config is None:
            self.config = event.config()
        else:
            self.config = config
        self.faker = faker.Faker()
        self.event_list = deque()
        for i in range(self.config.event_pool_size):
            is_invalid_event = random.random() < self.config.invalid_event_prob
            e = self.init_event(is_invalid_event=is_invalid_event)
            self.event_list.append(e)
        self.kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, default=self.enum_serializer).encode('utf-8'))

    
    def init_event(self, is_invalid_event=False):
        order_id = self.faker.uuid4()
        event_type = event_type_enum.ORDER_PENDING
        timestamp = self.faker.date_time()
        payload = self.generate_payload()
        return event(order_id, event_type, timestamp, payload, is_invalid_event=is_invalid_event)
    
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
    
    def next_event(self):

        time.sleep(1.0 / self.config.event_per_sec)
        

        if not self.event_list:
            return None
        current_event = self.event_list.popleft()
        next_event = current_event.generate_next_event(self.faker.date_time())
        if next_event is None:
            is_invalid_event = random.random() < self.config.invalid_event_prob
            next_event = self.init_event(is_invalid_event=is_invalid_event)
        self.event_list.append(next_event)
        self.kafka_producer.send('order_events', value=next_event.__dict__)

    


    

