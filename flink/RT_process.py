

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy, Row, Duration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.file_system import FileSink, RollingPolicy, Encoder, OutputFileConfig, BucketAssigner
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor, StateTtlConfig
from pyflink.datastream import OutputTag, CheckpointingMode
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.java_gateway import get_gateway


import sys
import os
import json
from data.events import event_type_enum

print("=== MODULE LOAD DEBUG ===")
print("Executable:", sys.executable)
print("Working dir:", os.getcwd())
print("SYS.PATH:")
for p in sys.path:
    print("  ", p)

import config as cfg




named_row = Types.ROW_NAMED(
    [
        "event_id",
        "order_id",
        "event_type",
        "timestamp",
        "customer_id",
        "product_id",
        "quantity",
        "price",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.INT(),
        Types.FLOAT(),
    ],
)



def clean_print(ds):

    os.system('cls' if os.name == 'nt' else 'clear')
    for dicts in cfg.console_messages.keys():
        print(f"{dicts}: ")
        for event_type, count in cfg.console_messages[dicts].items():
            print(f"{event_type}: {count}")
    sys.stdout.flush()

class validate_event(KeyedProcessFunction):

    invalid_side_output_tag = OutputTag("invalid_events", Types.STRING())
    dup_side_output_tag = OutputTag("duplicate_events", Types.STRING())


    def open(self, runtime_context):
        descriptor = ValueStateDescriptor("event_state", Types.STRING())
        map_descriptor = MapStateDescriptor("event_map_state", Types.STRING(), Types.STRING())

        ttlConfig = StateTtlConfig.new_builder(Time.minutes(10))\
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite)\
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired)\
            .build()

        self.event_state = runtime_context.get_state(descriptor)
        self.event_map_state = runtime_context.get_map_state(map_descriptor)
        self.event_map_state.enable_time_to_live(ttlConfig)
        self.event_state.enable_time_to_live(ttlConfig)

    def process_element(self, value, ctx):
        prev_state_name = self.event_state.value()
        event = json.loads(value)
        
        if self.event_map_state is not None:

            if self.event_map_state.contains(event['event_id']):
                yield self.dup_side_output_tag, self.parse_event_csv(value)

        self.event_map_state.put(event['event_id'], event['event_type'])

        if prev_state_name is not None:
            new_state = event_type_enum.get_enum(event['event_type'])
            prev_state = event_type_enum.get_enum(prev_state_name)

            if new_state.value < prev_state.value:
#                event['valid'] = "invalid"
                yield self.invalid_side_output_tag, self.parse_event_csv(value)
            else:
#                event['valid'] = "valid"
                self.event_state.update(event['event_type'])                
                yield self.parse_event_csv(value)
        else:
#            event['valid'] = "valid"
            self.event_state.update(event['event_type'])                
            yield self.parse_event_csv(value)

    def parse_event(self, value):
        data = json.loads(value)

        return Row(
            event_id=data["event_id"],
            order_id=data["order_id"],
            event_type=data["event_type"],
            timestamp=data["timestamp"],
            customer_id=data["payload"]["customer_id"],
            product_id=data["payload"]["product_id"],
            quantity=int(data["payload"]["quantity"]),
            price=float(data["payload"]["price"]),
        )

    def parse_event_csv(self, value):
        data = json.loads(value)

        dat = (
            data["event_id"],
            data["order_id"],
            data["event_type"],
            data["timestamp"],
            data["payload"]["customer_id"],
            data["payload"]["product_id"],
            data["payload"]["quantity"],
            data["payload"]["price"],
        )

        return ",".join(map(str, dat))




if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file://" + os.path.join(cfg.PROJECT_ROOT, "jar", "flink-connector-kafka-4.0.1-2.0.jar"),\
                  "file://" + os.path.join(cfg.PROJECT_ROOT, "jar", "kafka-clients-4.1.1.jar"), \
                    "file://" + os.path.join(cfg.PROJECT_ROOT, "jar", "flink-s3-fs-hadoop-2.2.0.jar"))
    env.set_parallelism(1)
    env.enable_checkpointing(20000)

    checkpoint_config = env.get_checkpoint_config()

    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(120000)  


    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(cfg.WATERMARK_DELAY))\
                        .with_timestamp_assigner(lambda event, timestamp: json.loads(event)['timestamp'])

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics('order_events') \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    jvm = get_gateway().jvm
    J_assigner = jvm.org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner("yyyy-MM/dd/HH")
    S3_bucket_assigner = BucketAssigner(J_assigner)

    file_sink = FileSink\
                    .for_row_format(cfg.S3_RAW_OUTPUT_DIR, Encoder.simple_string_encoder("UTF-8"))\
                    .with_bucket_assigner(S3_bucket_assigner)\
                    .with_rolling_policy(RollingPolicy.default_rolling_policy(
                        rollover_interval = 1 * 60 * 1000,  # roll every 1 minutes
                        part_size = 1 * 1024 * 1024,  # roll after file size exceeds 1 MB
                        inactivity_interval = 1 * 60 * 1000      # roll if no new data arrives for 1 minutes
                    ))\
                    .with_output_file_config(OutputFileConfig.builder().with_part_suffix(".csv").build())\
                    .build()

    invalid_events_sink = FileSink\
                    .for_row_format(cfg.S3_OUTPUT_DIR_INVALID_EVENTS, Encoder.simple_string_encoder("UTF-8"))\
                    .with_bucket_assigner(S3_bucket_assigner)\
                    .with_rolling_policy(RollingPolicy.default_rolling_policy(
                        rollover_interval = 1 * 60 * 1000,  # roll every 1 minutes
                        part_size = 1 * 1024 * 1024,  # roll after file size exceeds 1 MB
                        inactivity_interval = 1 * 60 * 1000      # roll if no new data arrives for 1 minutes
                    ))\
                    .with_output_file_config(OutputFileConfig.builder().with_part_suffix(".csv").build())\
                    .build()

    valid_events_sink = FileSink\
                    .for_row_format(cfg.S3_OUTPUT_DIR_VALID_EVENTS, Encoder.simple_string_encoder("UTF-8"))\
                    .with_bucket_assigner(S3_bucket_assigner)\
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
        .window(TumblingProcessingTimeWindows.of(Time.seconds(cfg.WINDOW_SIZE)))\
        .reduce(lambda a, b: (a[0], a[1] + b[1]))

    
    processed_ds = ds.key_by(lambda x: json.loads(x)['order_id'])\
                    .process(validate_event(), output_type=Types.STRING())
    
    invalid_ds = processed_ds.get_side_output(validate_event.invalid_side_output_tag)

    dup_ds = processed_ds.get_side_output(validate_event.dup_side_output_tag)

    #valid_ds = processed_ds.filter(lambda x: json.loads(x)['valid'] == "valid" and not json.loads(x)['duplicate'])
    
    # processed_ds.filter(lambda x: json.loads(x)['valid'] == "invalid")\
    #     .sink_to(invalid_events_sink)

    invalid_ds.sink_to(invalid_events_sink)

    processed_ds.sink_to(valid_events_sink)

    def update_summary(result):
        summary = cfg.console_messages.setdefault("summary", {})
        event_type, count = result
        summary[event_type] = summary.get(event_type, 0) + count
        clean_print(summary)

    def update_stats(result):
        summary = cfg.console_messages.setdefault("other_stats", {})
        event_type, count = result
        summary[event_type] = summary.get(event_type, 0) + count
        clean_print(summary)


    dss.map(update_summary)
    
    dup_ds.map(lambda x: ("duplicates", 1))\
        .key_by(lambda x: x[0])\
        .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))\
        .reduce(lambda a, b: (a[0], a[1] + b[1]))\
        .map(update_stats)

    env.execute("order_events_processing")

    env.close()



    
