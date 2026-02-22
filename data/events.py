import random
from enum import Enum
from datetime import datetime
import uuid

class event_type_enum(Enum):
    ORDER_PENDING = 1
    ORDER_PAID = 2
    ORDER_SHIPPED = 3
    ORDER_DELIVERED = 4
    ORDER_CANCELLED = 5
    ORDER_PAID_CANCELLED = 6
    ORDER_RETURNED = 7
    ORDER_REFUNDED = 8

    @classmethod
    def get_enum(cls, value:str):
        
        if value in cls.__members__:
            return cls[value]

        raise ValueError(f"No enum found for value: {value}")




class event:

    cancelled_or_returned_prob = 0.1
    is_invalid_event = False
    
    def __init__(self, order_id, event_type, timestamp, payload, is_invalid_event=False):
        self.event_id = uuid.uuid4()
        self.order_id = order_id
        self.event_type = event_type
        self.timestamp = timestamp
        self.payload = payload
        self.is_invalid_event = is_invalid_event
        self.invalid_stage = random.randrange(2, 4)

    def options(self, cancelled_or_returned_prob=None):
        if cancelled_or_returned_prob is not None:
            self.cancelled_or_returned_prob = cancelled_or_returned_prob


    def generate_next_event(self, new_timestamp):
        next_event_type = None
        cancelled_or_returned = random.random() < self.cancelled_or_returned_prob

        if self.is_invalid_event and self.event_type.value >= self.invalid_stage:
            next_event_type = self.random_lower_event(self.event_type)
        elif self.event_type == event_type_enum.ORDER_PENDING and cancelled_or_returned:
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
        return event(self.order_id, next_event_type, new_timestamp, self.payload, is_invalid_event=self.is_invalid_event)
    
    def random_lower_event(self, event: event_type_enum):
        lower_events = [
            e for e in event_type_enum
            if e.value < event.value
        ]

        if not lower_events:
            return None  # no lower value exists

        return random.choice(lower_events)
    
    class config:
        event_per_sec = 10
        cancelled_or_returned_prob = 0.1
        invalid_event_prob = 0.05
        event_pool_size = 50
        duplicate_event_prob = 0.05
        no_of_events = 1000

        def __init__(self, event_per_sec=None, 
                     cancelled_or_returned_prob=None, 
                     invalid_event_prob=None, 
                     event_pool_size=None, 
                     duplicate_event_prob=None,
                     no_of_events=None):
            if event_per_sec is not None:
                self.event_per_sec = event_per_sec
            if cancelled_or_returned_prob is not None:  
                self.cancelled_or_returned_prob = cancelled_or_returned_prob
            if invalid_event_prob is not None:
                self.invalid_event_prob = invalid_event_prob
            if event_pool_size is not None:
                self.event_pool_size = event_pool_size
            if duplicate_event_prob is not None:
                self.duplicate_event_prob = duplicate_event_prob
            if no_of_events is not None:
                self.no_of_events = no_of_events




