from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.catalog import JdbcCatalog
from pyflink.shell import s_env, st_env, DataTypes
from pyflink.table.schema import Schema
from pyflink.table.descriptors import Schema, OldCsv, Jdbc,Kinesis
from pyflink.table.table_descriptor import TableDescriptor, FormatDescriptor


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)


table_env \
    .connect(
        Kinesis()
        .stream_name('dritchik-education-lse')
        .region('us-east-1')
        .access_key('my-access-key')
        .secret_key('my-secret-key')
        .property('aws.endpoint', 'https://kinesis.us-east-1.amazonaws.com')
    ) \
    .with_format('json') \
    .with_schema(
        Schema()
        .field('event_id', 'FLOAT')
        .field('ticker', 'STRING')
        .field('price', 'FLOAT')
        .field('stock', 'STRING')
        .field('event_time', 'TIMESTAMP')
    ) \
    .create_temporary_table('kinesis_table')

result = table_env \
    .from_path('kinesis_table') \
    .select('ticker, price, event_time')


#WRITING TO JDBC
environment_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings)

schema = Schema.new_builder()\
    .column("ticker", DataTypes.STRING())\
    .column("price", DataTypes.FLOAT())\
    .column("event_time", DataTypes.TIMESTAMP())\
    .build()

t_env.connect(Jdbc()
    .url("dritchik-test.cwrdxdm3e0wh.us-east-1.rds.amazonaws.com")
    .username("dritchik")
    .password("dritchik")
    .table_name("flink_table")
    .schema(schema)
).with_properties({'driver': 'org.postgresql.Driver'}).create_temporary_table('flink_table_source')

t_env.from_elements(result,["ticker","price","event_time"]).insert_into("flink_table_source")
t_env.execite()


