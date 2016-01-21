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
                print("cur ts is %d, run limit is %d" % (rec.ts, run_limit))
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

def average_down_list(orig_list, max_len):
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

def scale_down_list(orig_list, max_len):
    orig_len = len(orig_list)
    scale = int(orig_len / max_len)
    l = []
    cur = 0
    while cur < orig_len:
        l.append(orig_list[cur])
        cur += scale
    if len(l) < max_len:
        l.append(orig_list[-1])
    return l

def visualize_space(args, space_history):
    from graphscreen import GraphWindow

    # Completely arbitrary value
    maximum_values = 4096

    total_times = sorted(list(space_history.total_hist.keys()))
    total_vals = []
    for ts in total_times:
        total_vals.append(space_history.total_hist[ts])

    used_times = sorted(list(space_history.used_hist.keys()))
    used_vals = []
    for ts in used_times:
        used_vals.append(space_history.used_hist[ts])

    reserved_times = sorted(list(space_history.reserved_hist.keys()))
    reserved_vals = []
    for ts in reserved_times:
        reserved_vals.append(space_history.reserved_hist[ts])

    readonly_times = sorted(list(space_history.readonly_hist.keys()))
    readonly_vals = []
    for ts in readonly_times:
        readonly_vals.append(space_history.readonly_hist[ts])

    if args.average:
        total_times = scale_down_list(total_times, maximum_values)
        total_vals = average_down_list(total_vals, maximum_values)
        used_times = scale_down_list(used_times, maximum_values)
        used_vals = average_down_list(used_vals, maximum_values)
        reserved_times = scale_down_list(reserved_times, maximum_values)
        reserved_vals = average_down_list(reserved_vals, maximum_values)
        readonly_times = scale_down_list(readonly_times, maximum_values)
        readonly_vals = average_down_list(readonly_vals, maximum_values)

    print(len(total_times))
    print(len(total_vals))
    print(len(used_times))
    print(len(used_vals))
    print(len(reserved_times))
    print(len(reserved_vals))
    window = GraphWindow()
    window.add_datapoints("Total", total_times, total_vals, (1, 1, 0))
    window.add_datapoints("Used", used_times, used_vals, (0, 1, 0))
    window.add_datapoints("Reserved", reserved_times, reserved_vals, (1, 0, 0))
    window.add_datapoints("Readonly", readonly_times, readonly_vals, (0, 0, 1))
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
