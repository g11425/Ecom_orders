import os


PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
OUTPUT_DIR_INVALID_EVENTS = os.path.join(PROJECT_ROOT, "output_invalid_events")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

if not os.path.exists(OUTPUT_DIR_INVALID_EVENTS):
    os.makedirs(OUTPUT_DIR_INVALID_EVENTS)