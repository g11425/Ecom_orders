from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.file_system import FileSink, RollingPolicy, Encoder, OutputFileConfig
from pyflink.common.time import Time
import config as cfg
import os
import json
import sys
    
def clean_print(ds):
    os.system('cls' if os.name == 'nt' else 'clear')
    for event_type, count in ds.items():
        print(f"{event_type}: {count}")
    sys.stdout.flush()

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

    ds.sink_to(file_sink)

    dss = ds.map(lambda x: (json.loads(x)['event_type'], 1))\
        .key_by(lambda x: x[0])\
        .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))\
        .reduce(lambda a, b: (a[0], a[1] + b[1]))
    

    summary = {}

    def update_summary(result):
        event_type, count = result
        summary[event_type] = summary.get(event_type, 0) + count
        clean_print(summary)

    dss.map(update_summary)

    env.execute("order_events_processing")

    env.close()



    
