from order_simulator import EventSimulator
from events import event

if __name__ == "__main__":
    conf = event.config( event_per_sec=1000, event_pool_size=50, invalid_event_prob=0.2, no_of_events=50000)
    simulator = EventSimulator(config=conf)
    while True:
        simulator.next_event()
        