from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.file_system import FileSink, RollingPolicy, Encoder, OutputFileConfig
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream import OutputTag
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
import config as cfg
import os
import json
import sys
from data.events import event_type_enum
    
def clean_print(ds):
    os.system('cls' if os.name == 'nt' else 'clear')
    for event_type, count in ds.items():
        print(f"{event_type}: {count}")
    sys.stdout.flush()

class validate_event(KeyedProcessFunction):

    side_output_tag = OutputTag("invalid_events", Types.STRING())

    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("event_state", Types.STRING())
        self.event_state = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        prev_state_name = self.event_state.value()
        event = json.loads(value)
        if prev_state_name is not None:
            new_state = event_type_enum.get_enum(event['event_type'])
            prev_state = event_type_enum.get_enum(prev_state_name)

            if new_state.value < prev_state.value:
                event['valid'] = "invalid"
                self.event_state.update(event['event_type'])                
                yield json.dumps(event) 
            else:
                event['valid'] = "valid"
                self.event_state.update(event['event_type'])                
                yield json.dumps(event) 
        else:
            event['valid'] = "valid"
            self.event_state.update(event['event_type'])                
            yield json.dumps(event) 



if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file://" + os.path.join(cfg.PROJECT_ROOT, "jar",\
                    "flink-connector-kafka-4.0.1-2.0.jar"), "file://" \
                    + os.path.join(cfg.PROJECT_ROOT, "jar", "kafka-clients-4.1.1.jar"))
    env.set_parallelism(1)
    env.enable_checkpointing(20000)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics('order_events') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    file_sink = FileSink\
                    .for_row_format(cfg.OUTPUT_DIR, Encoder.simple_string_encoder("UTF-8"))\
                    .with_rolling_policy(RollingPolicy.default_rolling_policy(
                        rollover_interval = 1 * 60 * 1000,  # roll every 1 minutes
                        part_size = 1 * 1024 * 1024,  # roll after file size exceeds 1 MB
                        inactivity_interval = 1 * 60 * 1000      # roll if no new data arrives for 1 minutes
                    ))\
                    .with_output_file_config(OutputFileConfig.builder().with_part_suffix(".csv").build())\
                    .build()

    invalid_events_sink = FileSink\
                    .for_row_format(cfg.OUTPUT_DIR_INVALID_EVENTS, Encoder.simple_string_encoder("UTF-8"))\
                    .with_rolling_policy(RollingPolicy.default_rolling_policy(
                        rollover_interval = 1 * 60 * 1000,  # roll every 1 minutes
                        part_size = 1 * 1024 * 1024,  # roll after file size exceeds 1 MB
                        inactivity_interval = 1 * 60 * 1000      # roll if no new data arrives for 1 minutes
                    ))\
                    .with_output_file_config(OutputFileConfig.builder().with_part_suffix(".csv").build())\
                    .build()


    ds.sink_to(file_sink)

    dss = ds.map(lambda x: (json.loads(x)['event_type'], 1))\
        .key_by(lambda x: x[0])\
        .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))\
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    ds.key_by(lambda x: json.loads(x)['order_id'])\
        .process(validate_event(), output_type=Types.STRING())\
        .filter(lambda x: json.loads(x)['valid'] == "invalid")\
        .sink_to(invalid_events_sink)

    summary = {}

    def update_summary(result):
        event_type, count = result
        summary[event_type] = summary.get(event_type, 0) + count
        clean_print(summary)

    dss.map(update_summary)

    env.execute("order_events_processing")

    env.close()



    
