#!/usr/bin/env python
# coding: utf-8

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.common.configuration import Configuration

import logging
import sys

from pyflink.common.time import Instant

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, TableDescriptor, Schema, StreamTableEnvironment)
from pyflink.table.expressions import lit, col
from pyflink.table.window import Slide

from pyflink.datastream.functions import FilterFunction


# 1. create a TableEnvironment

configuration = Configuration()

configuration.set_string("table.exec.state.ttl", "10m")

settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .with_configuration(configuration) \
        .build()


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_env = StreamTableEnvironment.create(stream_execution_environment=env, environment_settings=settings)

env.add_jars("file:/C:/Code/flink-sql-connector-kafka-1.16.0.jar")
env.add_jars("file:/C:/Code/flink-connector-jdbc-1.16.0.jar")
env.add_jars("file:/C:/Code/postgresql-42.5.1.jar")

t_env.execute_sql("""
    CREATE TABLE TransactionTable (
        `block_timestamp` BIGINT,
        `type` STRING,
        `hash` STRING,
        `nonce` BIGINT,
        `transaction_index` BIGINT,
        `from_address` STRING,
        `to_address` STRING,
        `value` BIGINT,
        `gas` BIGINT,
        `gas_price` BIGINT,
        `input` STRING,
        `block_number` BIGINT,
        `block_hash` STRING,
        `max_fee_per_gas` STRING,
        `max_priority_fee_per_gas` STRING,
        `transaction_type` INT,
        `receipt_cumulative_gas_used` BIGINT,
        `receipt_gas_used` BIGINT,
        `receipt_contract_address` STRING,
        `receipt_root` STRING,
        `receipt_status` INT,
        `receipt_effective_gas_price` BIGINT,
        `item_id` STRING,
        `item_timestamp` DATE,
        ts_ltz AS TO_TIMESTAMP_LTZ(block_timestamp * 1000, 3),
        WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '3' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions',
        'properties.bootstrap.servers' = '192.168.31.38:9094',
        'properties.group.id' = 'test_group_01',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
""")

t_env.execute_sql("""
    CREATE TABLE TokenTransferTable (
        `type` STRING,
        `token_address` STRING,
        `from_address` STRING,
        `to_address` STRING,
        `value` BIGINT,
        `transaction_hash` STRING,
        `log_index` INT,
        `block_number` BIGINT,
        `block_timestamp` BIGINT,
        `block_hash` STRING,
        `item_id` STRING,
        `item_timestamp` DATE,
        ts_ltz AS TO_TIMESTAMP_LTZ(block_timestamp * 1000, 3),
        WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '3' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'token_transfers',
        'properties.bootstrap.servers' = '192.168.31.38:9094',
        'properties.group.id' = 'test_group_01',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
""")



t_env.execute_sql("""
    CREATE TABLE JDBCSink (
        `L_time` TIMESTAMP(3),
        `L_hash` STRING,
        `L_block_hash` STRING,
        `R_block_hash` STRING,
        `log_index` INT,
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/tsdb',
        'table-name' = 'ods_trade_data',
        'username' = 'timescaledb',
        'password' = 'password'
    )

""")

t_env.execute_sql("""
    CREATE TABLE print (
        `L_time` TIMESTAMP(3),
        `L_hash` STRING,
        `L_block_hash` STRING,
        `R_block_hash` STRING,
        `log_index` INT,
        `window_start` TIMESTAMP(3),
        `window_end` TIMESTAMP(3)
    ) WITH (
        'connector' = 'print'
    )
""")


#transaction_table = t_env.from_path("TransactionTable")
#token_transfer_table = t_env.from_path("TokenTransferTable")


#result_table = transaction_table.select(col("block_timestamp"), col("from_address"))


tablefilter = t_env.sql_query(
    """
    SELECT L.ts_ltz as L_time, L.hash as L_hash, L.block_hash as L_block_hash, R.block_hash as R_block_hash,  R.log_index as log_index,
    COALESCE(L.window_start, R.window_start) as window_start,
    COALESCE(L.window_end, R.window_end) as window_end
    FROM (
    SELECT * FROM TABLE(TUMBLE(TABLE TransactionTable, DESCRIPTOR(ts_ltz), INTERVAL '3' SECOND))
    ) L
    LEFT JOIN (
    SELECT * FROM TABLE(TUMBLE(TABLE TokenTransferTable, DESCRIPTOR(ts_ltz), INTERVAL '3' SECOND))
    ) R
    ON L.hash = R.transaction_hash AND L.window_start = R.window_start AND L.window_end = R.window_end;
    """
)


#class TokenFilter(FilterFunction):
#    def filter(self, value):
#        return value["L_hash"] == '0x1010267dc21b55d256b3660f4cca65d4a27085b8a6219b846518654f8ef893f9'

# tablefilter.filter(col("L_hash") == '0x1010267dc21b55d256b3660f4cca65d4a27085b8a6219b846518654f8ef893f9').execute_insert('print').wait()

#table convert to datastream
# datastreamtmp = t_env.to_data_stream(tablefilter)
# datastreamtmp.filter(TokenFilter()).print()

statement_set = t_env.create_statement_set()

#tablefilter.filter(col("L_hash") == '0x1010267dc21b55d256b3660f4cca65d4a27085b8a6219b846518654f8ef893f9').execute_insert('print').wait()

tablefiledata = tablefilter.filter(col("L_hash") == '0x1010267dc21b55d256b3660f4cca65d4a27085b8a6219b846518654f8ef893f9')

statement_set.add_insert("JDBCSink", tablefiledata)
statement_set.add_insert("print", tablefiledata)

statement_set.execute().wait()

#env.execute()






