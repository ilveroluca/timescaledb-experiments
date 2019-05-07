#!/usr/bin/env python3

import argparse
import contextlib
import fileinput
import logging
import json
import os
import psycopg2
import psycopg2.extras  # need to import this explicitly
import random
import statistics
import sys
import threading
import time

from collections import defaultdict, deque
from queue import Queue

if psycopg2.__version__ < '2.7':
    raise ImportError("psycopg2 version >= 2.7 is required")


class Monitor(object):
    def __init__(self, name=None):
        self._name = name
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

    @property
    def name(self):
        return self._name

    def to_dict(self):
        return dict(
            name=self._name,
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
        else:
            raise ValueError("data dictionary does not contain all required keys")

        self._name = values_dictionary.get("name")
        return self

    @staticmethod
    def from_dict(d):
        m = Monitor()
        m.load_from_dict(d)
        return m

    def start(self, event_name):
        self._start_times[event_name] = time.time()

    def stop(self, event_name):
        stop_time = time.time()
        delta = stop_time - self._start_times[event_name]
        self._timings[event_name].append( (stop_time, delta) )
        return delta

    def collect_value(self, event_name, value):
        self._values[event_name].append( (time.time(), value) )

    def count(self, event_name, value=1):
        self._counters[event_name] += value

    @property
    def counter_names(self):
        return self._counters.keys()

    @property
    def value_names(self):
        return self._values.keys()

    @property
    def timing_names(self):
        return self._timing.keys()

    def get_count(self, event_name):
        return self._counters.get(event_name, 0)

    def get_values(self, name):
        return self._values.get(name, [])

    def get_timings(self, name):
        return self._timings.get(name, [])

    def has_counter(self, event_name):
        event_name in self._counters

    def each_counter(self):
        for k, v in self._counters.items():
            yield k, v

    def each_value_list(self):
        for k, v in self._values.items():
            yield k, v

    def each_timing_list(self):
        for k, v in self._timings.items():
            yield k, v

    def write_summary(self):
        sary = []
        if self._name:
            sary.append("Monitor: {}".format(self._name))

        if len(self._counters) > 0:
            sary.append("Counters:")
            for name, count in self._counters.items():
                sary.append("    {}: {}".format(name, count))

        def write_collection(values_dict):
            result = []
            for name, ary in values_dict.items():
                result.append("    {}: n={}; mean={}; stdev={}; sum={}".format(
                    name, len(ary),
                    "{:0.3f}".format(statistics.mean( t[1] for t in ary )) if len(ary) > 0 else "n/a",
                    "{:0.3f}".format(statistics.stdev( t[1] for t in ary )) if len(ary) > 1 else "n/a",
                    "{:0.3f}".format(sum( t[1] for t in ary ))))
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


@contextlib.contextmanager
def quick_cursor(db_connection_params, commit=False):
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
        if commit:
            conn.commit()
        conn.close()


def pretty_print_json_results(json_filename):
    with open(json_filename) as f:
        data = json.load(f)
    for element in data:
        m = Monitor.from_dict(element)
        print(m.write_summary())


class JsonLineFileInputReader(object):
    def __init__(self, queue, files=None, batch_size=5000, repeat_input_times=1):
        self._queue = queue
        self._files = files if files else ["-"]
        self._batch_size = batch_size
        self._repeats = repeat_input_times
        self._monitor = Monitor("JsonLineFileInputReader")

        if self._repeats > 1 and self._files == ["-"]:
            raise ValueError("Repeats different from 1 are not supported when reading from stdin")
        if self._repeats < 1:
            raise ValueError("Repeats must be >= 1 (got {})".format(self._repeats))

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


class DBSchema(object):
    def __init__(self, creation_sql):
        self._create_sql = creation_sql
        self._queries = dict()

    def create(self, cur):
        logging.info("Executing schema SQL")
        cur.execute(self._create_sql)

    def add_query(self, name, sql):
        self._queries[name] = sql

    def iterqueries(self):
        for name, sql in self._queries.items():
            yield name, sql


class DBOperator(object):
    def __init__(self, db_connection_params, name=None):
        self._conn = psycopg2.connect(
            host=db_connection_params.host,
            port=db_connection_params.port,
            dbname=db_connection_params.dbname,
            user=db_connection_params.username,
            password=db_connection_params.password)

        if name:
            monitor_name = name
        else:
            monitor_name = type(self).__name__
        self._monitor = Monitor(monitor_name)

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def get_monitor(self):
        return self._monitor


class DBIngester(DBOperator, threading.Thread):
    def __init__(self, db_connection_params, q, name=None):
        DBOperator.__init__(self, db_connection_params, name)
        threading.Thread.__init__(self)
        self._q = q

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
                    self._monitor.collect_value("inserts thus far", self._monitor.get_count("records inserted"))
                    self._monitor.collect_value("inserts/sec", len(batch) / delta )
                finally:
                    self._q.task_done()
        finally:
            self._conn.close()

    def run(self):
        try:
            self.write()
        except Exception as e:
            self._monitor.exception = e
            raise

    def _insert_batch(self, batch):
        raise NotImplementedError()


class DBIngesterSchemaJson(DBIngester):
    @classmethod
    def get_schema(cls):
        schema = DBSchema(cls.schema_sql())

        q = dict()
        q['reading_types'] = """
            select x from source, jsonb_to_recordset(source.fields) as x(type text, fieldname text) group by x;
        """

        q['sources'] = """
            select
                source_serial_no,
                x.fieldname,
                description
            from source, jsonb_to_recordset(source.fields) as x(type text, fieldname text);
        """

        q['active_sources_in_area'] = """
            select source_serial_no
            from source s
            where exists(
              select source_serial_no
              from reading r join source s using (source_serial_no)
              where
                  r.time >= '2019-04-17 20:00:00' and r.time < '2019-04-18 20:00:00'
                  and ST_Distance(s.geom, ST_SetSRID(ST_MakePoint(9, 39),3003)) < 0.2);
        """

        q['source_timeseries'] = """
            select
                time, r.source_serial_no, (data->>'environment.humidity')::real
            from reading r
            where
                source_serial_no = 'probe-8'
                and r.time >= '2019-04-17 20:00:00' and r.time < '2019-04-18 20:00:00';
        """

        q['source_timebucket'] = """
            select time_bucket('5 minute', time) AS five_min, avg((data->>'environment.humidity')::real)
            from reading r
            where
                source_serial_no = 'probe-8'
                and r.time >= '2019-04-17 20:00:00' and r.time < '2019-04-18 20:00:00'
            GROUP BY five_min;
        """

        for name, sql in q.items():
            schema.add_query(name, sql)

        return schema

    def schema_sql():
        q = """
    drop table if exists reading cascade;
    drop table if exists source_reading_type cascade;
    drop table if exists source cascade;
    drop table if exists reading_type cascade;
    drop table if exists reading_category cascade;
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    CREATE EXTENSION IF NOT EXISTS postgis CASCADE;

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
        source_serial_no text not null,
        fields jsonb not null,
        geom geometry,
        description jsonb,
        primary key (source_serial_no)
    );

    create table source_reading_type (
        source_serial_no text not null references source,
        reading_category text not null,
        reading_type text not null,
        primary key (source_serial_no, reading_category, reading_type),
        foreign key (reading_category, reading_type) references reading_type(category, reading_type)
    );

    create table reading (
        time timestamp(0) not null,
        source_serial_no text not null references source,
        data jsonb,
        tiledb text,
        tiledb_coord text,
        constraint check_payload check (data is not null or (tiledb is not null and tiledb_coord is not null))
    );

    --select create_hypertable('reading', 'time', chunk_time_interval => interval '1 day');
    select create_hypertable('reading', 'time', create_default_indexes => false, chunk_time_interval => interval '1 day');
    create index on reading (source_serial_no, time DESC);
        """
        n_sources = 16
        q += "insert into source (source_serial_no, fields, geom) values "
        rtypes = [ 'environment.humidity', 'environment.radiation_level', 'environment.ambient_temperature', 'environment.photosensor' ]
        field_spec = json.dumps([ { 'fieldname': n, "type": "real" } for n in rtypes ])
        values = [
            """('probe-{:x}', '{}', ST_GeomFromText('POINT({:0.6f} {:0.6f})', 3003))""".format(
                n, field_spec, 9 + n / 100, 39 + n / 100)
            for n in range(n_sources)
        ]
        q += ",\n".join(values) + ";\n"

        q += "insert into source_reading_type (source_serial_no, reading_category, reading_type) values"
        values = [
            """('probe-{:x}', '{}', '{}')""".format(n, *r_type.split('.'))
            for n in range(n_sources) for r_type in rtypes
        ]
        q += ",\n".join(values) + ";\n"
        return q

    def _insert_batch(self, batch):
        logging.debug("Inserting DBIngesterSchemaJson batch")

        def _make_tuple(item):
            ts = item.pop('timestamp')
            sensor = item.pop('sensor_uuid')[0:7]  # take the first 7 characters to make an id like 'probe-a'
            new_item = {}
            for k, v in item.items():
                new_item['environment.' + k] = v  # prepend category 'environment'
            return (ts, sensor, psycopg2.extras.Json(new_item))

        sql = "insert into reading (time, source_serial_no, data) values %s"
        tuples = [ _make_tuple(item) for item in batch ]
        template = "(to_timestamp(%s), %s, %s)"
        with self._monitor.time_block("insert query time"):
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=500)
            self._conn.commit()


class DBIngesterSchemaValues(DBIngester):
    @classmethod
    def get_schema(cls):
        schema = DBSchema(cls.schema_sql())

        q = dict()
        q['reading_types'] = """
            select category || '.' || reading_type from reading_type;
        """

        q['sources'] = """
            select
                source_serial_no,
                reading_category,
                reading_type,
                description
            from source;
        """

        q['active_sources_in_area'] = """
            select source_serial_no
            from source s
            where exists(
              select source_serial_no
              from reading r join source s using (source_serial_no)
              where
                  r.time >= '2019-04-17 20:00:00' and r.time < '2019-04-18 20:00:00'
                  and ST_Distance(s.geom, ST_SetSRID(ST_MakePoint(9, 39),3003)) < 0.2);
        """

        q['source_timeseries'] = """
            select
                time, r.source_serial_no, value
            from reading r
            join source s using (source_serial_no)
            where
                source_serial_no = 'probe-8-h'
                and r.time >= '2019-04-17 20:00:00' and r.time < '2019-04-18 20:00:00';
        """

        q['source_timebucket'] = """
            select time_bucket('5 minute', time) AS five_min, avg(value)
            from reading r
            where
                source_serial_no = 'probe-8-h'
                and r.time >= '2019-04-17 20:00:00' and r.time < '2019-04-18 20:00:00'
            GROUP BY five_min;
        """

        for name, sql in q.items():
            schema.add_query(name, sql)

        return schema

# max time 2019-04-19 08:41:18
# min time 2019-04-17 16:41:10

    def schema_sql():
        q = """
    drop table if exists reading cascade;
    drop table if exists source cascade;
    drop table if exists source_reading_type cascade;
    drop table if exists reading_type cascade;
    drop table if exists reading_category cascade;
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
    CREATE EXTENSION IF NOT EXISTS postgis CASCADE;

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
        source_serial_no text not null,
        reading_category text not null,
        reading_type text not null,
        geom geometry,
        description jsonb,
        primary key (source_serial_no),
        foreign key (reading_category, reading_type) references reading_type(category, reading_type)
    );

    create table reading (
        time timestamp(0) not null,
        source_serial_no text not null references source,
        value real,
        tiledb text,
        tiledb_coord text,
        constraint check_payload check (value is not null or (tiledb is not null and tiledb_coord is not null))
    );

    --select create_hypertable('reading', 'time', chunk_time_interval => interval '1 day');
    select create_hypertable('reading', 'time', create_default_indexes => false, chunk_time_interval => interval '1 day');
    create index on reading (source_serial_no, time DESC);
    insert into source (source_serial_no, reading_category, reading_type, geom) values
        """
        rtypes = [ 'humidity', 'radiation_level', 'ambient_temperature', 'photosensor' ]
        sources = [
            "('probe-{:x}-{}', 'environment', '{}', ST_GeomFromText('POINT({:0.6f} {:0.6f})', 3003))".format(
                n, t[0:1], t, 9 + n / 100, 39 + n / 100)
            for n in range(16) for t in rtypes
        ]
        q += ",\n".join(sources) + ";"
        return q

    def _insert_batch(self, batch):
        logging.debug("Inserting DBIngesterSchemaValues batch")

        def _make_tuples(item):
            ts = item.pop('timestamp')
            sensor = item.pop('sensor_uuid')
            return [ (ts, "{}-{}".format(sensor, key[0:1]), value) for key, value in item.items() ]

        sql = "insert into reading (time, source_serial_no, value) values %s"
        tuples = [ t for item in batch for t in _make_tuples(item) ]
        template = "(to_timestamp(%s), %s, %s)"
        with self._monitor.time_block("insert query time"):
            with self._conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, tuples, template=template, page_size=500)
            self._conn.commit()


class QueryOperator(DBOperator, threading.Thread):
    def __init__(self, db_connection_params, schema_obj, name=None):
        DBOperator.__init__(self, db_connection_params)
        threading.Thread.__init__(self)
        self._schema_obj = schema_obj
        self._run = True
        self._interval = db_connection_params.query_interval
        self._stop_event = threading.Event()

    @property
    def interval(self):
        return self._interval

    @interval.setter
    def set_interval(self, v):
        if v <= 0:
            raise ValueError("sleep interval must be > 0")
        self._interval = v

    def stop(self):
        logging.debug("QueryOperator.stop()")
        self._run = False
        self._stop_event.set()

    def _do_work(self):
        logging.debug("Query operator starting")
        while self._run:
            for name, sql in self._schema_obj.iterqueries():
                if not self._run:
                    break
                with self._monitor.time_block("query_" + name):
                    with self._conn.cursor() as cur:
                        logging.debug("Running %s query", name)
                        cur.execute(sql)
                        # fetch records
                        rows = cur.fetchall()  # brutally risky, but I think it'll be ok for our tests
                        logging.info("Fetched %s rows", len(rows))
                        rows = None
                self._stop_event.wait(self._interval)
        logging.debug("Query operator exiting")

    def run(self):
        try:
            self._do_work()
        except Exception as e:
            self._monitor.exception = e
            raise


def report_table_sizes(db_conn_options, monitor):
    logging.debug("Getting table and index sizes from DB")
    with quick_cursor(db_conn_options) as cur:
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
            monitor.collect_value(row[0], row[1])
        monitor.collect_value("total size", sum( r[1] for r in rows ))


def write_output(filename, all_monitors):
    logging.info("Writing output file %s", filename)
    with open(filename, 'w') as f:
        json.dump([ m.to_dict() for m in all_monitors ], f)

    for monitor in all_monitors:
        logging.info(monitor.write_summary())


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
    parser.add_argument('--query-interval', metavar="N", type=int, default=5, help="N. seconds between read queries (per thread)")
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
    logging.info("Ingeter schema class: %s", ingester_class.__name__)
    schema_obj = ingester_class.get_schema()

    logging.info("Creating DB tables")

    with quick_cursor(options, True) as cur:
        schema_obj.create(cur)
    logging.info("DB ready")

    def input_worker(batch_reader):
        try:
            batch_reader.read()
        except Exception as e:
            batch_reader.get_monitor().exception = e
            raise
        finally:
            results_q.append(batch_reader.get_monitor())

    threads = []

    with global_monitor.time_block("full operation"):
        logging.debug("Launching reader thread")
        t = threading.Thread(target=input_worker, args=(reader,))
        t.start()
        threads.append(t)

        for i in range(options.num_writers):
            logging.debug("Launching writer thread %d", i)
            t = ingester_class(options, batch_q)
            threads.append(t)
            t.start()

        for i in range(options.num_query):
            time.sleep(2 * random.random())  # Stagger query op starts
            query_op = QueryOperator(options, schema_obj)
            threads.append(query_op)
            logging.debug("Launching query thread %d", i)
            query_op.start()

        logging.debug("main thread joining reader")
        threads[0].join()  # join the reading thread
        logging.debug("main thread now joining job queue")
        batch_q.join()  # wait for all batches to be processed

    logging.info("work finished.")

    logging.debug("Stopping writer threads")
    for i in range(options.num_writers):
        batch_q.put(None)

    logging.debug("Stopping query threads and joining all")
    for t in threads:
        if hasattr(t, 'stop'):
            t.stop()
        t.join()

    logging.debug("All threads joined")

    logging.debug("Collecting results")
    # threads[0] is the reader thread, which is different from the others and has already appended its monitor
    # to our collection
    for t in threads[1:]:
        results_q.append(t.get_monitor())

    logging.debug("Checking for errors")
    for mon in results_q:
        if mon.exception:
            logging.fatal("Exception in thread")
            logging.exception(mon.exception)
            sys.exit(1)

    report_table_sizes(options, global_monitor)

    logging.info("Preparing output")
    results_q.append(global_monitor)

    write_output(options.output, results_q)

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
