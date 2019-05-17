#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import sys

from ingest_pubnub import Monitor

# from https://matplotlib.org/gallery/ticks_and_spines/multiple_yaxis_with_spines.html


def transpose(timestamped_values):
    return list(map(list, zip(*timestamped_values)))


def make_patch_spines_invisible(ax):
    ax.set_frame_on(True)
    ax.patch.set_visible(False)
    for sp in ax.spines.values():
        sp.set_visible(False)


def map_timeseries_to_table_size(sorted_inserts_thus_far, sorted_timeseries):

    def iter_last_two(timeseries):
        if len(timeseries) == 0:
            return

        it_inserts = iter(timeseries)
        previous = next(it_inserts)
        if len(timeseries) == 1:
            yield previous, None
            return

        for e in it_inserts:
            yield previous, e
            previous = e
        else:
            yield e, None

    inserts_it = iter_last_two(sorted_inserts_thus_far)
    current_rec, next_rec = next(inserts_it)

    ts_idx = 0
    while ts_idx < len(sorted_timeseries):
        ts_record = sorted_timeseries[ts_idx]
        if next_rec is not None:
            if ts_record[0] < next_rec[0]:
                yield [ current_rec[1] ] + ts_record[1:]
                ts_idx += 1
            else:
                current_rec, next_rec = next(inserts_it)
        else:
            yield [ current_rec[1] ] + ts_record[1:]
            ts_idx += 1


def collect_data(filename):
    with open(filename) as f:
        data = json.load(f)
    mdict = dict( [ (d['name'], Monitor.from_dict(d)) for d in data ] )

    result = dict()

    m = mdict.get('DBIngesterSchemaJson', mdict.get('DBIngesterSchemaValues'))
    if m is None:
        raise RuntimeError("DBIngesterSchemaJson and DBIngesterSchemaValues are missing from data")

    # We had four rows in the Reading relation per input record ( 'humidity', 'radiation_level', 'ambient_temperature', 'photosensor' )
    inserts_timeseries = [ (ts, inserts * 4) for ts, inserts in m.get_values("inserts thus far") ]

    def prep_series_for_plot(sorted_timeseries):
        return transpose([ x for x in map_timeseries_to_table_size(inserts_timeseries, sorted_timeseries) ])

    label = "inserts/sec"
    result[label] = prep_series_for_plot(m.get_values(label))

    label = "insert query time"
    result[label] = prep_series_for_plot(m.get_timings(label))

    m = mdict['QueryOperator']

    for label in ("query_reading_types",
                  "query_sources",
                  "query_active_sources_in_area",
                  "query_source_timeseries",
                  "query_source_timebucket"):
        result[label] = prep_series_for_plot(m.get_timings(label))

    return result


def main(filename):

    data = collect_data(filename)

    #  ## query time plots ###

    series_and_labels = {
        #  "query_reading_types": "Value types",
        "query_sources": "All sensor ids",
        "query_active_sources_in_area": "Active sensors in geogr area",
        "query_source_timeseries": "Data one sensor 1 day",
        "query_source_timebucket": "Data one sensor 1 day 5-min bucket avg" }

    f = plt.figure()
    plt.ylabel("Milliseconds")
    plt.xlabel("DB size (rows)")

    for key, label in series_and_labels.items():
        plt.plot(data[key][0], data[key][1], '-', label=label)

    plt.legend(fontsize='small')
    plt.show()
    f.savefig("query_timings.pdf", bbox_intext='tight')
    plt.close()

    #  ## insert time plot ###

    series_and_labels = {
        #  "query_reading_types": "Value types",
        "inserts/sec": "Readings ingested/sec" }

    label = "inserts/sec"
    ymax = max(data[label][1])
    # ymin = min(data[label][1])

    f = plt.figure()
    plt.ylabel("Readings/sec")
    plt.xlabel("DB size (rows)")

    plt.ylim(0, 1.3 * ymax)

    for key, label in series_and_labels.items():
        plt.plot(data[key][0], data[key][1], '.', label=label)

    plt.legend(fontsize='small')
    plt.show()
    f.savefig("insert_timings.pdf", bbox_intext='tight')
    plt.close()


if __name__ == "__main__":
    main(sys.argv[1])
