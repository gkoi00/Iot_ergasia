import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.time_characteristic import TimeCharacteristic
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.datastream.connectors.pulsar import DeliveryGuarantee
from pyflink.datastream.output_tag import OutputTag
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.state import ValueStateDescriptor
from datetime import datetime
from typing import Tuple
from collections.abc import Iterable

def sensors():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.add_jars("file:///home/george/iot_ergasia/flink-sql-connector-kafka-1.17.1.jar")
    env.add_jars("file:///home/george/iot_ergasia/flink-connector-redis_2.12-1.1.0.jar")
    
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    source = KafkaSource.builder().set_bootstrap_servers('localhost:9092').set_topics('iot_measurements').set_value_only_deserializer(SimpleStringSchema()).set_starting_offsets(KafkaOffsetsInitializer.earliest()).build()
    
    ds = env.from_source(source, watermark_strategy=WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_days(3)), source_name='iot_measurements')
    
    sink = KafkaSink.builder() \
    .set_bootstrap_servers('localhost:9092') \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("Aggregations")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()

    late_output_tag = OutputTag("late-data", type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    def split(line):
        line = line[1:-1]
        return line

    class SumAggregate(AggregateFunction):
        
        def create_accumulator(self) -> Tuple[str, str, float]:
            return "", "", 0

        def add(self, value: Tuple[str, str, float], accumulator: Tuple[str, str, float]) -> Tuple[str, str, float]:
            if value[0] == "Mov1":
                return "Other", "", 0
            elif value[0] == "Wtot" or value[0] == "Etot":
                return "AggDayDiff" + value[0], value[1], abs(value[2] - accumulator[2])
            else:
                return "AggDay" + value[0], max(value[1], accumulator[1]), accumulator[2] + value[2]

        def get_result(self, accumulator: Tuple[str, str, float]) -> Tuple[str, str, float]:
            return accumulator[0], accumulator[1], accumulator[2]

        def merge(self, a: Tuple[str, str, float], b: Tuple[str, str, float]) -> Tuple[str, str, float]:
            return a[0], a[1], a[2]

    class PreAggregate(AggregateFunction):

        def create_accumulator(self) -> Tuple[str, str, float, str]:
            return "", "", 0, ""

        def add(self, value: Tuple[str, str, float], accumulator: Tuple[str, str, float, str]) -> Tuple[str, str, float, str]:
            if value[0] == "AggDayDiffEtot" or value[0] == "AggDayHVAC1" or value[0] == "AggDayHVAC2" or value[0] == "AggDayMiAC1" or value[0] == "AggDayMiAC2":
                return value[0], value[1], value[2], "AggDayRest1"
            elif value[0] == "AggDayDiffWtot" or value[0] == "AggDayW1":
                return value[0], value[1], value[2], "AggDayRest2"
            else:
                return value[0], value[1], value[2], "Other"

        def get_result(self, accumulator: Tuple[str, str, float, str]) -> Tuple[str, str, float, str]:
            return accumulator[0], accumulator[1], accumulator[2], accumulator[3]

        def merge(self, a: Tuple[str, str, float], b: Tuple[str, str, float]) -> Tuple[str, str, float, str]:
            return a[0], a[1], a[2], a[3]

    class RestAggregate(AggregateFunction):

        def create_accumulator(self) -> Tuple[str, str, float]:
            return "", "", 0

        def add(self, value: Tuple[str, str, float, str], accumulator: Tuple[str, str, float]) -> Tuple[str, str, float]:
            if  value[3] == "AggDayRest1":
                if value[0] == "AggDayDiffEtot":
                    return "AggDayRestE", value[1], accumulator[2] + value[2]
                else:
                    return "AggDayRestE", value[1], accumulator[2] - value[2]
            elif  value[3] == "AggDayRest2":
                if value[0] == "AggDayDiffWtot":
                    return "AggDayRestW", value[1], accumulator[2] + value[2]
                else:
                    return "AggDayRestW", value[1], accumulator[2] - value[2]
            else:
                return "Other", "", 0

        def get_result(self, accumulator: Tuple[str, str, float, str]) -> Tuple[str, str, float]:
            return accumulator[0], accumulator[1], accumulator[2]

        def merge(self, a: Tuple[str, str, float], b: Tuple[str, str, float]) -> Tuple[str, str, float]:
            return a[0], a[1], a[2]

    ds = ds.map(lambda value: (value[1:-1].split()[0], value[1:-1].split()[1] + " " + value[1:-1].split()[2], float(value[1:-1].split()[-1])), output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
            .key_by(lambda i: i[0]) \
            .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-1.75))) \
            .side_output_late_data(late_output_tag) \
            .aggregate(SumAggregate(),
                    accumulator_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]),
                    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]))

    ds1 = ds.map(lambda value: value[0] + " " + value[1] + " " + str(value[2]), output_type=Types.STRING())
    ds1.sink_to(sink)

    late_stream = ds.get_side_output(late_output_tag) \
            .map(lambda value: "Late" + value[0] + " " + value[1] + " " + str(value[2]), output_type=Types.STRING())
    late_stream.sink_to(sink)

    ds2 = ds.key_by(lambda i: i[0]) \
            .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-1.75))) \
            .aggregate(PreAggregate(),
                    accumulator_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]),
                    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING()])) \
            .key_by(lambda i: i[3]) \
            .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-1.75))) \
            .aggregate(RestAggregate(),
                    accumulator_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()]),
                    output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT()])) \
            .map(lambda value: value[0] + " " + value[1] + " " + str(value[2]),
                    output_type=Types.STRING())

    #ds2.print()
    
    ds2.sink_to(sink)
    
    # submit for execution
    env.execute()

sensors()
