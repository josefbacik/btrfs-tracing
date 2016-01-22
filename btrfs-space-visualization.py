import os
import argparse
import subprocess
import gc
import sys
import time
from tracecmd import Trace
from ctracecmd import pevent_register_comm
from ctracecmd import pevent_data_comm_from_pid
from ctracecmd import py_supress_trace_output
from ctracecmd import tracecmd_buffer_instances
from ctracecmd import tracecmd_buffer_instance_handle

NSECS_IN_SEC = 1000000000
reservations = {}
block_groups = []
space_infos = []
flush_events = []

class SpaceHistory:
    def __init__(self):
        self.used_bytes = 0
        self.reserved_bytes = 0
        self.readonly_bytes = 0
        self.total_bytes = 0

        self.total_hist = {}
        self.used_hist = {}
        self.reserved_hist = {}
        self.readonly_hist = {}

        self.total_times = []
        self.total_vals = []
        self.used_times = []
        self.used_vals = []
        self.reserved_times = []
        self.reserved_vals = []
        self.readonly_times = []
        self.readonly_vals = []
        self.enabled = True

    def add_space(self, ts, total=0, used=0, reserved=0, readonly=0):
        if not self.enabled:
            return
        self.total_bytes += total
        self.total_hist[ts] = self.total_bytes
        self.used_bytes += used
        self.used_hist[ts] = self.used_bytes
        self.reserved_bytes += reserved
        self.reserved_hist[ts] = self.reserved_bytes
        self.readonly_bytes += readonly
        self.readonly_hist[ts] = self.readonly_bytes

    def remove_space(self, ts, total=0, used=0, reserved=0, readonly=0):
        if not self.enabled:
            return
        self.total_bytes -= total
        self.total_hist[ts] = self.total_bytes
        self.used_bytes -= used
        self.used_hist[ts] = self.used_bytes
        self.reserved_bytes -= reserved
        self.reserved_hist[ts] = self.reserved_bytes
        self.readonly_bytes -= readonly
        self.readonly_hist[ts] = self.readonly_bytes

    def _build_list(self, hist, ts_start, ts_end):
        times = sorted(list(hist.keys()))
        vals = []
        start = None
        end = 0
        for i in range(0, len(times)):
            ts = times[i]
            if ts_start > 0:
                if ts < ts_start:
                    continue
                elif start is None:
                    start = i
            if ts_end > 0 and ts > ts_end:
                end = i
                break
            vals.append(hist[ts])
        if start is not None:
            times = times[start:end]
        return times, vals

    def _average_down_list(self, orig_list, max_len):
        orig_len = len(orig_list)
        if orig_len <= max_len:
            return orig_list
        scale = int(len(orig_list) / max_len)
        l = []
        value = 0
        cur = 0
        for i in orig_list:
            value += i
            cur += 1
            if cur == scale:
                l.append(int(value / scale))
                cur = 0
                value = 0
        if cur:
            l.append(int(value / cur))
        return l

    def _scale_down_list(self, orig_list, max_len):
        orig_len = len(orig_list)
        if orig_len <= max_len:
            return orig_list
        scale = int(orig_len / max_len)
        l = []
        cur = 0
        while cur < orig_len:
            l.append(orig_list[cur])
            cur += scale
        if len(l) < max_len:
            l.append(orig_list[-1])
        return l

    def build_lists(self, max_vals=0, ts_start=0, ts_end=0):
        self.total_times, self.total_vals = self._build_list(self.total_hist, ts_start, ts_end)
        self.used_times, self.used_vals = self._build_list(self.used_hist, ts_start, ts_end)
        self.reserved_times, self.reserved_vals = self._build_list(self.reserved_hist, ts_start, ts_end)
        self.readonly_times, self.readonly_vals = self._build_list(self.readonly_hist, ts_start, ts_end)

        if max_vals:
            self.total_times = self._scale_down_list(self.total_times, max_vals)
            self.total_vals = self._average_down_list(self.total_vals, max_vals)
            self.used_times = self._scale_down_list(self.used_times, max_vals)
            self.used_vals = self._average_down_list(self.used_vals, max_vals)
            self.reserved_times = self._scale_down_list(self.reserved_times, max_vals)
            self.reserved_vals = self._average_down_list(self.reserved_vals, max_vals)
            self.readonly_times = self._scale_down_list(self.readonly_times, max_vals)
            self.readonly_vals = self._average_down_list(self.readonly_vals, max_vals)

class Blockgroup:
    def __init__(self, offset, size):
        self.offset = offset;
        self.size = size;

class Spaceinfo:
    BTRFS_BLOCK_GROUP_DATA = (1 << 0)
    BTRFS_BLOCK_GROUP_METADATA = (1 << 1)
    BTRFS_BLOCK_GROUP_SYSTEM = (1 << 2)

    def __init__(self, flags):
        self.flags = flags & (self.BTRFS_BLOCK_GROUP_DATA |
                              self.BTRFS_BLOCK_GROUP_METADATA |
                              self.BTRFS_BLOCK_GROUP_SYSTEM)
        self.size = 0
        self.bytes_used = 0
        self.bytes_readonly = 0
        self.bytes_may_use = 0

    def add_block_group(self, size, bytes_used, bytes_super):
        self.size += size
        self.bytes_used += bytes_used
        self.bytes_readonly += bytes_super

def find_block_group(offset):
    for  block_group in block_groups:
        if (block_group.offset <= offset) and ((block_group.offset + block_group.size) > offset):
            return block_group
    return None

def find_space_info(flags):
    for space_info in space_infos:
        if space_info.flags == flags:
            return space_info
    space_info = Spaceinfo(flags)
    space_infos.append(space_info)
    return space_info

def parse_tracefile(args, space_history):
    trace = Trace(args.infile)

    cpustats = trace.cpustats()

    # The format is "Buffer: name\n\n\nCpu0: blah\n\nCpu1: blah\n\n"
    cpus = cpustats.split('\n\n\n')
    cpus = cpus[1].split('\n\n')

    instances = tracecmd_buffer_instances(trace._handle)
    if instances != 0:
        new_handle = tracecmd_buffer_instance_handle(trace._handle, 0)
        trace._handle = new_handle

    total_events = 0
    for cpu in range(0, trace.cpus):
        stats = dict(item.split(':') for item in cpus[cpu].split("\n"))
        total_events += int(stats['read events'])

    print("Total events %d" % (total_events))
    cur_event = 0
    obj_count = 0
    start_time = time.time()
    rem = 0
    run_limit = -1

    while True:
        if obj_count > 100000:
            now = time.time()
            t = now - start_time
            start_time = now
            t /= obj_count
            rem = total_events - cur_event
            rem *= t
            gc.collect()
            obj_count = 1
        sys.stdout.write("\r%d - %s seconds remaining" % (cur_event, rem))
        sys.stdout.flush()
        rec = trace.read_next_event()
        if rec is None:
            break

        # First figure out if we have a run time limit
        if run_limit == -1:
            if args.time:
                run_limit = rec.ts + (args.time * NSECS_IN_SEC)
            else:
                run_limit = 0
        if run_limit > 0 and rec.ts > run_limit:
            break
        cur_event += 1
        obj_count += 1
        if rec.name == "btrfs_add_block_group":
            space_history.add_space(rec.ts, total=rec.num_field("size"),
                                    used=rec.num_field("bytes_used"),
                                    readonly=rec.num_field("bytes_super"))

            space_info = find_space_info(rec.num_field("flags"))
            space_info.add_block_group(rec.num_field("size"),
                                       rec.num_field("bytes_used"),
                                       rec.num_field("bytes_super"))
            block_group = Blockgroup(rec.num_field("offset"),
                                     rec.num_field("size"))
            block_groups.append(block_group)
            block_group.space_info = space_info
        if rec.name == "btrfs_space_reservation":
            reserve_type = rec.str_field("type")
            if "enospc" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                print("Hit enospc, dumping info\n")
                for r,val in reservations:
                    print("%s: %d\n" % (r, val))
                print("Space info %d, may_use %d, used %d, readonly %d\n" %
                        (space_info.flags, space_info.bytes_may_use,
                         space_info.bytes_used, space_info.bytes_readonly))
            elif "space_info" in reserve_type:
                space_info = find_space_info(rec.num_field("val"))
                if rec.num_field("reserve") == 1:
                    space_info.bytes_may_use += rec.num_field("bytes")
                    space_history.add_space(rec.ts,
                                            reserved=rec.num_field("bytes"))
                else:
                    space_history.remove_space(rec.ts,
                                               reserved=rec.num_field("bytes"))
                    space_info.bytes_may_use -= rec.num_field("bytes")
            if reserve_type not in reservations:
                reservations[reserve_type] = 0
            if rec.num_field("reserve") == 1:
                reservations[reserve_type] += rec.num_field("bytes")
            else:
                reservations[reserve_type] -= rec.num_field("bytes")
            del reserve_type
        if rec.name == "btrfs_reserved_extent_alloc" or rec.name == "btrfs_reserved_extent_free":
            block_group = find_block_group(rec.num_field("start"))
            if not block_group:
                print("Huh, didn't find a block group for %d" %
                        (rec.num_field("start")))
                continue
            space_info = block_group.space_info
            if rec.name == "btrfs_reserved_extent_alloc":
                space_history.add_space(rec.ts, used=rec.num_field("len"))
                space_info.bytes_used += rec.num_field("len")
            else:
                space_history.remove_space(rec.ts, used=rec.num_field("len"))
                space_info.bytes_used -= rec.num_field("len")
        if rec.name == "btrfs_trigger_flush":
            flush_events.append([rec.ts, rec.name, rec.str_field("reason")])
        if rec.name == "btrfs_flush_space":
            flush_events.append([rec.ts, rec.name, string(rec.num_field("state"))])
    # If we had a run limit we don't want to do the leak detection as it will be
    # wrong
    if run_limit > 0:
        return

    num_leaks = 0
    for space_info in space_infos:
        if space_info.bytes_may_use != 0:
            print("Bytes may use leak for space info %d, bytes_may_use %d"  %
                  (space_info.flags, space_info.bytes_may_use))
            num_leaks += 1
    for name,value in reservations.iteritems():
        if value != 0:
            print("Reservation for %s outstanding, value %d" % (name, value))
            num_leaks += 1

    if num_leaks == 0:
        print("Yay no leaks!")


def rescale_cb(area, space_history, ts_start, ts_end):
    space_history.build_lists(4096, ts_start, ts_end)
    area.update_datapoints("Total", space_history.total_times, space_history.total_vals)
    area.update_datapoints("Used", space_history.used_times, space_history.used_vals)
    area.update_datapoints("Reserved", space_history.reserved_times, space_history.reserved_vals)
    area.update_datapoints("Readonly", space_history.readonly_times, space_history.readonly_vals)

def visualize_space(args, space_history):
    from graphscreen import GraphWindow
    max_vals = 0
    if args.average:
        # A completely arbitrary limit
        max_vals = 4096
    space_history.build_lists(max_vals)

    window = GraphWindow()
    window.set_rescale_cb(rescale_cb, space_history)
    window.add_datapoints("Total", space_history.total_times, space_history.total_vals, (1, 1, 0))
    window.add_datapoints("Used", space_history.used_times, space_history.used_vals, (0, 1, 0))
    window.add_datapoints("Reserved", space_history.reserved_times, space_history.reserved_vals, (1, 0, 0))
    window.add_datapoints("Readonly", space_history.readonly_times, space_history.readonly_vals, (0, 0, 1))
    for events in flush_events:
        print("adding an event to the liststore")
        window.liststore.append(events)
    window.main()

def record_events():
    events = [ "btrfs:btrfs_add_block_group",
               "btrfs:btrfs_space_reservation",
               "btrfs:btrfs_reserved_extent_alloc",
               "btrfs:btrfs_reserved_extent_free",
               "btrfs:btrfs_trigger_flush",
               "btrfs:btrfs_flush_space",
             ]

    cmd = [ 'trace-cmd', 'record', '-B', 'enospc', '-b', '20480', ]
    for e in events:
        cmd.extend(['-e', e])
    subprocess.call(cmd)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Visualizer for space usage "+
                                     "in btrfs during operation.")
    parser.add_argument('-i', '--infile', type=str, default="trace.dat",
                        help="Process a given trace.dat file")
    parser.add_argument('-r', '--record', action='store_true',
                        help="Record events that we can replay later")
    parser.add_argument('-c', '--nogtk', action='store_true',
                        help="Don't display gtk window, just do the leak check")
    parser.add_argument('-t', '--time', type=int,
                        help="Limit the parsing to the given amount of seconds")
    parser.add_argument('-a', '--average', action='store_true',
                        help="Average a large dataset over its time series")
    args = parser.parse_args()

    if args.record:
        record_events()
    else:
        py_supress_trace_output()
        space_history = SpaceHistory()
        if args.nogtk:
            space_history.enabled = False
        parse_tracefile(args, space_history)
        if not args.nogtk:
            visualize_space(args, space_history)
