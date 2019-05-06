#!/usr/bin/env python3

import argparse
import contextlib
import fileinput
import logging
import json
import os
import psycopg2
import psycopg2.extras  # need to import this explicitly
import statistics
import sys
import threading
import time

from collections import defaultdict, deque
from queue import Queue

if psycopg2.__version__ < '2.7':
    raise ImportError("psycopg2 version >= 2.7 is required")


class Monitor(object):
    def __init__(self):
        self._counters = defaultdict(int)
        self._start_times = {}
        self._timings = defaultdict(list)
        self._values = defaultdict(list)
        self._exception = None

    @property
    def exception(self):
        return self._exception

    @exception.setter
    def exception(self, v):
        self._exception = v

    def to_dict(self):
        return dict(
            counters=self._counters,
            timings=self._timings,
            values=self._values)

    def load_from_dict(self, values_dictionary):
        if set( ('counters', 'timings', 'values') ) <= values_dictionary.keys():
            self._counters.clear()
            self._counters.update(values_dictionary['counters'])
            self._timings.clear()
            self._timings.update(values_dictionary['timings'])
            self._values.clear()
            self._values.update(values_dictionary['values'])
            return self
        else:
            raise ValueError("data dictionary does not contain all required keys")

    def start(self, event_name):
        self._start_times[event_name] = time.time()

    def stop(self, event_name):
        delta = time.time() - self._start_times[event_name]
        self._timings[event_name].append(delta)
        return delta

    def collect_value(self, event_name, value):
        self._values[event_name].append(value)

    def count(self, event_name, value=1):
        self._counters[event_name] += value

    def has_counter(self, event_name):
        event_name in self._counters

    def each_counter(self):
        for k, v in self._counters.items():
            yield k, v

    def each_value_list(self):
        for k, v in self._values.items():
            yield k, v

    def write_summary(self):
        sary = []

        if len(self._counters) > 0:
            sary.append("Counters:")
            for name, count in self._counters.items():
                sary.append("    {}: {}".format(name, count))

        def write_collection(values_dict):
            result = []
            for name, values in values_dict.items():
                result.append("    {}: n={}; mean={}; stdev={}; sum={}".format(
                    name, len(values),
                    "{:0.3f}".format(statistics.mean(values)) if len(values) > 0 else "n/a",
                    "{:0.3f}".format(statistics.stdev(values)) if len(values) > 1 else "n/a",
                    "{:0.3f}".format(sum(values))))
            return result

        if len(self._timings) > 0:
            sary.append("Timings:")
            sary.extend(write_collection(self._timings))

        if len(self._values) > 0:
            sary.append("Values:")
            sary.extend(write_collection(self._values))

        return "\n".join(sary)

    class TimingBlock(object):
        def __init__(self, monitor, event_name):
            self.__monitor = monitor
            self.__event_name = event_name

        def __enter__(self):
            self.__monitor.start(self.__event_name)
            return self.__monitor

        def __exit__(self, exception_type, exception_val, exception_tb):
            self.__monitor.stop(self.__event_name)
            return False

    def time_block(self, event_name):
        """
        Use in a with statement to time a block of code.  Example:
          with monitor.time_block("my_event"):
              do_something()
        """
        return type(self).TimingBlock(self, event_name)


class JsonLineFileInputReader(object):
    def __init__(self, queue, files=None, batch_size=5000, repeat_input_times=1):
        self._queue = queue
        self._files = files if files else ["-"]
        self._batch_size = batch_size
        self._repeats = repeat_input_times
        self._monitor = Monitor()

    def read(self):
        batch = []
        max_time = 0
        min_time = sys.maxsize
        sensor_name_length = len("probe-x")

        def close_batch():
            nonlocal batch
            self._monitor.stop("reading batch")
            self._monitor.count("records read", len(batch))
            self._monitor.count("batches")
            qsize = self._queue.qsize()
            self._monitor.collect_value("queue size before insert", qsize)
            logging.debug("Closing batch.  Queue size: %d", qsize)
            self._queue.put(batch)
            batch = []

        for i in range(self._repeats):
            logging.info("Reading data (repeat %d of %d)", i, self._repeats)
            self._repeats -= 1
            # shift input forward by the time interval covered by the previous repeats
            timestamp_offset = 0 if max_time == 0 else (max_time - min_time)
            logging.debug("timestamp offset: %d", timestamp_offset)
            with fileinput.FileInput(files=self._files, openhook=fileinput.hook_compressed) as fi:
                logging.debug("Starting batch")
                self._monitor.start("reading batch")
                for line in fi:
                    item = json.loads(line)
                    item['sensor_uuid'] = item['sensor_uuid'][0:sensor_name_length]

                    int_time = int(item['timestamp'])
                    if max_time < int_time:
                        max_time = int_time
                    if min_time > int_time:
                        min_time = int_time

                    item['timestamp'] = int_time + timestamp_offset
                    batch.append(item)
                    if len(batch) == self._batch_size:
                        close_batch()
                        self._monitor.start("reading batch")
                else:
                    close_batch()
                    self._monitor.start("reading batch")

    def get_monitor(self):
        return self._monitor


class DBIngester(object):
    def __init__(self, db_connection_params, q):
        self._q = q
        self._conn = psycopg2.connect(
            host=db_connection_params.host,
            port=db_connection_params.port,
            dbname=db_connection_params.dbname,
            user=db_connection_params.username,
            password=db_connection_params.password)
        self._monitor = Monitor()

    @classmethod
    def create_schema(cls, db_connection_params):
        logging.info("Connecting to DB")
        conn = psycopg2.connect(
            host=db_connection_params.host,
            port=db_connection_params.port,
            dbname=db_connection_params.dbname,
            user=db_connection_params.username,
            password=db_connection_params.password)
        try:
            logging.info("Executing schema SQL")
            with conn.cursor() as cur:
                cur.execute(cls.schema_sql())
            conn.commit()
            logging.info("Tables created")
        finally:
            conn.close()

    def write(self):
        try:
            while True:
                batch = self._q.get()
                try:
                    if batch is None:
                        break
                    self._monitor.start("insert time")
                    self._insert_batch(batch)
                    delta = self._monitor.stop("insert time")
                    self._monitor.count("records inserted", len(batch))
                    self._monitor.collect_value("inserts/sec", len(batch) / delta )
                finally:
                    self._q.task_done()
        finally:
            self._conn.close()

    def _insert_batch(self, batch):
        raise NotImplementedError()

    def get_monitor(self):
        return self._monitor


@contextlib.contextmanager
def quick_cursor(db_connection_params):
    logging.debug("Connecting to DB")
    conn = psycopg2.connect(
        host=db_connection_params.host,
        port=db_connection_params.port,
        dbname=db_connection_params.dbname,
        user=db_connection_params.username,
        password=db_connection_params.password)
    try:
        yield conn.cursor()
    finally:
        conn.close()


def pretty_print_json_results(json_filename):
    with open(json_filename) as f:
        data = json.load(f)
    for element in data:
        m = Monitor().load_from_dict(element)
        print(m.write_summary())


class DBIngesterSchemaJson(DBIngester):
    @classmethod
    def schema_sql():
        return """
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    drop table if exists reading cascade;
    drop table if exists source cascade;
    drop table if exists reading_type cascade;
    drop table if exists reading_category cascade;

    create table reading_category (
        category text not null,
        primary key(category),
        constraint category_lowercase check (category = lower(category))
    );

    insert into reading_category values ('environment');

    create table reading_type (
        category text not null references reading_category(category),
        reading_type text not null,
        primary key (category, reading_type),
        constraint type_lowercase check (reading_type = lower(reading_type))
    );
    insert into reading_type (category, reading_type) values
        ('environment', 'humidity'),
        ('environment', 'radiation_level'),
        ('environment', 'ambient_temperature'),
        ('environment', 'photosensor');


    create table source (
        source_id text not null primary key
    );

    create table reading (
        time timestamp(0) not null,
        source_id text not null references source,
        data jsonb,
        tiledb text,
        tiledb_coord text,
        constraint check_payload check (data is not null or (tiledb is not null and tiledb_coord is not null))
    );
    insert into source (source_id) values
        ('probe-0'),
        ('probe-1'),
        ('probe-2'),
        ('probe-3'),
        ('probe-4'),
        ('probe-5'),
        ('probe-6'),
        ('probe-7'),
        ('probe-8'),
        ('probe-9'),
        ('probe-a'),
        ('probe-b'),
        ('probe-c'),
        ('probe-d'),
        ('probe-e'),
        ('probe-f');

    --select create_hypertable('reading', 'time', chunk_time_interval => interval '1 day');
    select create_hypertable('reading', 'time', create_default_indexes => false, chunk_time_interval => interval '1 day');
    create index on reading (source_id, time DESC);
        """

    def _insert_batch(self, batch):
        logging.debug("Inserting DBIngesterSchemaJson batch")

        def _make_tuple(item):
            ts = item.pop('timestamp')
            sensor = item.pop('sensor_uuid')
            return (ts, sensor, psycopg2.extras.Json(item))

        sql = "insert into reading (time, source_id, data) values %s"
        tuples = [ _make_tuple(item) for item in batch ]
        template = "(to_timestamp(%s), %s, %s)"
        with self._monitor.time_block("insert query time"):
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=500)
            self._conn.commit()


class DBIngesterSchemaValues(DBIngester):
    def schema_sql():
        q = """
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    drop table if exists reading cascade;
    drop table if exists source cascade;
    drop table if exists reading_type cascade;
    drop table if exists reading_category cascade;

    create table reading_category (
        category text not null,
        primary key(category),
        constraint category_lowercase check (category = lower(category))
    );

    insert into reading_category values ('environment');

    create table reading_type (
        category text not null references reading_category(category),
        reading_type text not null,
        primary key (category, reading_type),
        constraint type_lowercase check (reading_type = lower(reading_type))
    );
    insert into reading_type (category, reading_type) values
        ('environment', 'humidity'),
        ('environment', 'radiation_level'),
        ('environment', 'ambient_temperature'),
        ('environment', 'photosensor');

    create table source (
        source_id text not null,
        reading_category text not null,
        reading_type text not null,
        primary key (source_id),
        foreign key (reading_category, reading_type) references reading_type(category, reading_type)
    );

    create table reading (
        time timestamp(0) not null,
        source_id text not null references source,
        value real,
        tiledb text,
        tiledb_coord text,
        constraint check_payload check (value is not null or (tiledb is not null and tiledb_coord is not null))
    );

    --select create_hypertable('reading', 'time', chunk_time_interval => interval '1 day');
    select create_hypertable('reading', 'time', create_default_indexes => false, chunk_time_interval => interval '1 day');
    create index on reading (source_id, time DESC);
    insert into source (source_id, reading_category, reading_type) values
        """
        rtypes = [ 'humidity', 'radiation_level', 'ambient_temperature', 'photosensor' ]
        sources = [ "('probe-{:x}-{}', 'environment', '{}')".format(n, t[0:1], t) for n in range(16) for t in rtypes ]
        q += ",\n".join(sources) + ";"
        return q

    def _insert_batch(self, batch):
        logging.debug("Inserting DBIngesterSchemaValues batch")

        def _make_tuples(item):
            ts = item.pop('timestamp')
            sensor = item.pop('sensor_uuid')
            return [ (ts, "{}-{}".format(sensor, key[0:1]), value) for key, value in item.items() ]

        sql = "insert into reading (time, source_id, value) values %s"
        tuples = [ t for item in batch for t in _make_tuples(item) ]
        template = "(to_timestamp(%s), %s, %s)"
        with self._monitor.time_block("insert query time"):
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=500)
            self._conn.commit()


def get_ingester_class(choice_text):
    if choice_text == 'values':
        return DBIngesterSchemaValues
    elif choice_text == 'json':
        return DBIngesterSchemaJson
    else:
        raise ValueError("Unknown ingester selection values")


def main(args):
    logging.basicConfig(level=logging.DEBUG)

    batch_size = 5000
    max_queue_length = 5

    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--host', metavar="HOST", required=True, help="DB host")
    parser.add_argument('-p', '--port', metavar="PORT", default=5432, help="DB port")
    parser.add_argument('-d', '--dbname', metavar="DBNAME", required=True, help="DB name")
    parser.add_argument('-U', '--username', metavar="USERNAME", required=True, help="DB username")
    parser.add_argument('-W', '--password', metavar="PASSWORD", required=True, help="DB password")
    parser.add_argument('-o', '--output', metavar="OUTPUT", required=True, help="File to which to write resulting stats")
    parser.add_argument('-r', '--repeats', metavar="N", type=int, default=1, help="how many times to repeat the input data")
    parser.add_argument('-w', '--num-writers', metavar="N", type=int, default=1, help="Number of writer threads")
    parser.add_argument('-q', '--num-query', metavar="N", type=int, default=1, help="Number of query threads")
    parser.add_argument('--ingester-schema', choices=('values', 'json'), metavar="C", default='values', help="Select the schema to test")

    options, left_over = parser.parse_known_args(args)
    logging.debug("Options: %s", options)
    logging.debug("left_over: %s", left_over)

    if os.path.exists(options.output):
        parser.error("output file {} already exists. Won't overwrite".format(options.output))

    # Test write to make sure it works
    with open(options.output, 'w'):
        pass

    batch_q = Queue(max_queue_length)
    results_q = deque()  # use a deque because it's thread-safe. SimpleQueue isn't available on python < 3.7
    global_monitor = Monitor()

    logging.debug("Creating reader")
    reader = JsonLineFileInputReader(batch_q, left_over if left_over else None,
                                     batch_size, options.repeats)

    ingester_class = get_ingester_class(options.ingester_schema)

    logging.info("Creating DB tables")
    ingester_class.create_schema(options)
    logging.info("Tables ready")

    def input_worker(batch_reader):
        try:
            batch_reader.read()
        except Exception as e:
            batch_reader.get_monitor().exception = e
            raise
        finally:
            results_q.append(batch_reader.get_monitor())

    def writer_worker():
        try:
            ingester = ingester_class(options, batch_q)
            ingester.write()
        except Exception as e:
            ingester.get_monitor().exception = e
            raise
        finally:
            results_q.append(ingester.get_monitor())

    threads = []

    with global_monitor.time_block("full operation"):
        logging.debug("Launching reader thread")
        t = threading.Thread(target=input_worker, args=(reader,))
        t.start()
        threads.append(t)

        for i in range(options.num_writers):
            logging.debug("Launching writer thread %d", i)
            t = threading.Thread(target=writer_worker)
            threads.append(t)
            t.start()

        logging.debug("main thread joining reader")
        threads[0].join()  # join the reading thread
        logging.debug("main thread now joining job queue")
        batch_q.join()  # wait for all batches to be processed
    # Stop writer threads
    logging.info("work finished.  Stopping writer threads")
    for i in range(options.num_writers):
        batch_q.put(None)

    logging.debug("Checking for errors")
    for mon in results_q:
        if mon.exception:
            logging.fatal("Exception in thread")
            logging.exception(mon.exception)
            sys.exit(1)

    logging.debug("Getting table and index sizes from DB")
    with quick_cursor(options) as cur:
        size_threshold = 1000000  # we filter all relations and indexes smaller than this value
        query = """
            SELECT nspname || '.' || relname AS "relation",
                 pg_relation_size(C.oid) AS "size"
               FROM pg_class C
               LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
               WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND pg_relation_size(C.oid) > {}
               ORDER BY pg_relation_size(C.oid) DESC;""".format(size_threshold)
        #  WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND pg_relation_size(C.oid) > 1000000
        cur.execute(query)
        rows = cur.fetchall()
        if len(rows) == 0:
            logging.info("All relation sizes filtered since they are under the size threshold of %s", size_threshold)
        for row in rows:
            global_monitor.collect_value(row[0], row[1])
        global_monitor.collect_value("total size", sum( r[1] for r in rows ))

    logging.info("Preparing output")
    results_q.append(global_monitor)

    logging.info("Writing output file %s", options.output)
    with open(options.output, 'w') as f:
        json.dump([ m.to_dict() for m in results_q ], f)

    for monitor in results_q:
        logging.info(monitor.write_summary())

    with quick_cursor(options) as cur:
        # Get hypertable sizes from TimescaleDB
        # These values are already included in the counts, but they're pretty-printed here
        query = "select table_name, table_size, index_size from timescaledb_information.hypertable;"
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            logging.info("hypertable.%s.table_size = %s", row[0], row[1])
            logging.info("hypertable.%s.index_size = %s", row[0], row[2])


if __name__ == '__main__':
    main(sys.argv[1:])
