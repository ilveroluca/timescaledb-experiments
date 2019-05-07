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


def collect_data(filename):
    with open(filename) as f:
        data = json.load(f)
    mdict = dict( [ (d['name'], Monitor.from_dict(d)) for d in data ] )

    result = dict()

    m = mdict['DBIngesterSchemaJson']

    for label in ("inserts thus far", "inserts/sec"):
        result[label] = transpose(m.get_values(label))

    label = "insert query time"
    result[label] = transpose(m.get_timings(label))

    m = mdict['QueryOperator']
    label = "total size"
    result[label] = transpose(m.get_values(label))
    # Scale total size down to MB
    result[label][1] = [ y / 2**20 for y in result[label][1] ]

    for label in ("query_reading_types",
                  "query_sources",
                  "query_active_sources_in_area",
                  "query_source_timeseries",
                  "query_source_timebucket"):
        result[label] = transpose(m.get_timings(label))

    return result


def main(filename):

    data = collect_data(filename)

    min_time = min( min(v[0]) for v in data.values() )
    max_time = max( max(v[0]) for v in data.values() )

    fig, host = plt.subplots()
    fig.subplots_adjust(right=0.6)

    # how many axes do we need?
    #
    # milliseconds "insert query time", query_*
    # hundreds: total size
    # thousands: "inserts/sec"
    # millions: "inserts thus far"

    # par1 = host.twinx()
    # par2 = host.twinx()

    hund = host.twinx()
    thou = host.twinx()
    millions = host.twinx()

    # Offset the right spine of par2.  The ticks and label have already been
    # placed on the right by twinx above.
    hund.spines["right"].set_position(("axes", 1.0))
    thou.spines["right"].set_position(("axes", 1.1))
    millions.spines["right"].set_position(("axes", 1.2))

    # Having been created by twinx, par2 has its frame off, so the line of its
    # detached spine is invisible.  First, activate the frame but make the patch
    # and spines invisible.
    for p in (hund, thou, millions):
        make_patch_spines_invisible(p)
        # Second, show the right spine.
        p.spines["right"].set_visible(True)

    ms_labels = [ "query_reading_types",
                  "query_sources",
                  "query_active_sources_in_area",
                  "query_source_timeseries",
                  "query_source_timebucket",
                  "insert query time" ]
    ms_plots = [ host.plot(data[label][0], data[label][1], label=label)[0] for label in ms_labels[0:-1] ]
    ms_plots += [ host.plot(data[label][0], data[label][1], "x", label=label)[0] for label in ms_labels[-1:] ]

    hund_labels = [ "total size" ]
    hund_plots = [ hund.plot(data[label][0], data[label][1], "ro", label=label)[0] for label in hund_labels ]

    thou_labels = [ "inserts/sec" ]
    thou_plots = [ thou.plot(data[label][0], data[label][1], "+", label=label)[0] for label in thou_labels ]

    millions_labels = [ "inserts thus far" ]
    millions_plots = [ millions.plot(data[label][0], data[label][1], "b.", label=label)[0] for label in millions_labels ]

    host.set_xlim(min_time, max_time)
    #  host.set_ylim(0, 2)
    #  par1.set_ylim(0, 4)
    #  par2.set_ylim(1, 65)

    host.set_xlabel("Time")
    host.set_ylabel("Milliseconds")
    hund.set_ylabel("Size MB")
    thou.set_ylabel("Inserts/sec")
    millions.set_ylabel("Total inserts")

    #  host.yaxis.label.set_color(p1.get_color())
    #  par1.yaxis.label.set_color(p2.get_color())
    #  par2.yaxis.label.set_color(p3.get_color())

    #  tkw = dict(size=4, width=1.5)
    #  host.tick_params(axis='y', colors=p1.get_color(), **tkw)
    #  par1.tick_params(axis='y', colors=p2.get_color(), **tkw)
    #  par2.tick_params(axis='y', colors=p3.get_color(), **tkw)
    #  host.tick_params(axis='x', **tkw)

    lines = ms_plots + hund_plots + thou_plots + millions_plots

    #  host.legend(lines, [l.get_label() for l in lines])

    plt.legend(lines, [l.get_label() for l in lines], fontsize='small', bbox_to_anchor=(0, 1.0))
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
