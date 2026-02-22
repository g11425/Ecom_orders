import os
import sys


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
OUTPUT_DIR_INVALID_EVENTS = os.path.join(PROJECT_ROOT, "output_invalid_events")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

if not os.path.exists(OUTPUT_DIR_INVALID_EVENTS):
    os.makedirs(OUTPUT_DIR_INVALID_EVENTS)

console_messages = {}

def clean_print(ds):

    os.system('cls' if os.name == 'nt' else 'clear')
    for dicts in console_messages.keys():
        print(f"{dicts}: ")
        for event_type, count in console_messages[dicts].items():
            print(f"{event_type}: {count}")
    sys.stdout.flush()
