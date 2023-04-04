from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import os
import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.window import Tumble
from pyflink.table.udf import ScalarFunction
from pyflink.table import expressions as E
from pyflink.table.window import Over
from pyflink.table.udf import udtf, udf, udaf, AggregateFunction, TableAggregateFunction, udtaf


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = StreamTableEnvironment.create(environment_settings=env_settings)

def create_table(table_name, stream_name, region):
    return """ CREATE TABLE {0} (
                event_id VARCHAR(60),
                ticker VARCHAR(6),
                price DOUBLE,
                stock VARCHAR(6),
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
              )
              PARTITIONED BY (ticker)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region)

def create_output_table(table_name, stream_name, region):
    return """ CREATE TABLE {0} (
                ticker VARCHAR(6),
                avg_price DOUBLE
              )
              PARTITIONED BY (ticker)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region)

def calculate_mean(table) :
    {

    }

def main():
    input_stream_key = "dritchik-education-lse"
    input_stream_key_2 = "dritchik-education-nasdaq"
    input_region_key = "us-east-1"

    output_stream_key = "dritchik-education-flink-output"
    output_region_key = "us-east-1"

    # tables
    input_table_name = "ExampleInputStream"
    input_table_name_2 = "ExampleInputStream_2"
    output_table_name = "ExampleOutputStream"

    # 2. Creates a source table from a Kinesis Data Stream
    table_env.execute_sql(create_table(input_table_name, input_stream_key, input_region_key))

    # 2.1 Creates a source table num 2 from a Kinesis Data Stream
    table_env.execute_sql(create_table(input_table_name_2, input_stream_key_2, input_region_key))

    # 3. Creates a sink table writing to a Kinesis Data Stream
    table_env.execute_sql(create_output_table(output_table_name, output_stream_key, output_region_key))

    # 4. Interval join
    left = table_env.from_path('ExampleInputStream').select(E.col('event_id'),E.col('ticker'), E.col('price'), E.col('stock'), E.col('event_time'))
    right = table_env.from_path('ExampleInputStream_2').select(E.col('event_id'),E.col('ticker'), E.col('price'), E.col('stock'), E.col('event_time'))

    joined_table = left.join(right.rename_columns(E.col('event_id').alias('event_id_2'),E.col('ticker').alias('ticker_2'), E.col('event_time').alias('event_time_2'), E.col('price').alias('price_2'),E.col('stock').alias('stock_2'))).where(
        (E.col('ticker') == E.col('ticker_2')) & (E.col('event_time') >= E.col('event_time_2') - E.lit(3).seconds) & (
                E.col('event_time') <= E.col('event_time_2') + E.lit(3).seconds))

    #5.1 Group by and avg value from 1 input table
    input = table_env.from_path("ExampleInputStream")
    result_1 = input.window(Tumble.over(E.lit(3).seconds).on(E.col('event_time')).alias("w")) \
        .group_by(E.col('w'),E.col('ticker')) \
        .select(E.col('ticker'), E.col('price').avg.alias('price'))

    # 5.2 Group by and avg value from 2 input table
    input_2 = table_env.from_path("ExampleInputStream_2")
    result_2 = input_2.window(Tumble.over(E.lit(3).seconds).on(E.col('event_time')).alias("w")) \
        .group_by(E.col('w'),E.col('ticker')) \
        .select(E.col('ticker'), E.col('price').avg.alias('price'))

    joined_table = result_1.join(result_2.rename_columns(E.col('ticker').alias('ticker_2'), E.col('price').alias('price_2'))).where(
        E.col('ticker') == E.col('ticker_2')
    )
    result = joined_table.select(E.col('ticker'), ((E.col('price') + E.col('price_2'))/2).alias('avg_price'))

    # 5. Inserts the source table data into the sink table
    table_result = table_env.execute_sql("INSERT INTO {0} SELECT * FROM {1}".format(output_table_name, result))

    # get job status through TableResult
    print(table_result.get_job_client())


if __name__ == '__main__':
    main()
