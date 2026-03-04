from order_simulator import EventSimulator
from events import event

if __name__ == "__main__":
    conf = event.config( event_per_sec=200, event_pool_size=10, invalid_event_prob=0.5)
    simulator = EventSimulator(config=conf)
    while True:
        simulator.next_event()
        