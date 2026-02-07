from order_simulator import EventSimulator

if __name__ == "__main__":
    simulator = EventSimulator()
    while True:
        simulator.next_event()
        